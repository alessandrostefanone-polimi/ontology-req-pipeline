from openai import OpenAI
from ontology_req_pipeline.data_models import NormalizedRecord
from typing import Dict, Any, Optional, Tuple, List
from pathlib import Path
from textwrap import dedent
from tempfile import NamedTemporaryFile
from collections import defaultdict
import hashlib
import json
import os
import re
import rdflib
from rdflib import OWL, RDF, URIRef, Literal, XSD
from owlapy.iri import IRI
from owlapy.owl_ontology import SyncOntology, Ontology, RDFLibOntology
from owlapy.owl_reasoner import SyncReasoner
from owlapy.owl_axiom import OWLObjectPropertyAssertionAxiom, OWLClassAssertionAxiom, OWLDataPropertyAssertionAxiom, OWLClass
from owlapy.owl_property import OWLObjectProperty, OWLDataProperty
from owlapy.owl_individual import OWLNamedIndividual
from owlapy.owl_literal import OWLLiteral

class AgenticKGBuilder:
    def __init__(
        self,
        tbox_path: Path,
        record: Any,
        reasoner: str = "Pellet",
        max_iters: int = 3,
        llm_provider: str = "openai",
        llm_model: Optional[str] = None,
    ):
        self.tbox_path = Path(tbox_path)
        self.llm_provider = str(llm_provider or "openai").strip().lower()
        if self.llm_provider not in {"openai", "ollama"}:
            raise ValueError("llm_provider must be either 'openai' or 'ollama'")
        self.model = (
            str(llm_model).strip()
            if isinstance(llm_model, str) and str(llm_model).strip()
            else ("gpt-5.1" if self.llm_provider == "openai" else "llama3.2")
        )
        self.payload = record.model_dump() if hasattr(record, 'model_dump') else json.loads(json.dumps(record))
        self.temperature = 0
        if self.llm_provider == "openai":
            self.client = OpenAI()
        else:
            self.client = OpenAI(
                base_url=os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1"),
                api_key=os.getenv("OLLAMA_API_KEY", "ollama"),
            )
        self.record = record
        self.reasoner = reasoner
        self.INFERRED_PATH = Path(f"final_KG_inferred_{self.payload.get('idx', 'demo')}.owl")
        self.max_iters = max_iters
        self.REPO_ROOT = Path.cwd()
        self.ONTOLOGY_DIR = (self.REPO_ROOT / "ontologies") if (self.REPO_ROOT / "ontologies").exists() else (self.REPO_ROOT.parent / "ontologies")
        self.CORE_PATH = self.tbox_path if self.tbox_path.exists() else (self.ONTOLOGY_DIR / "Core.rdf")
        self._tbox_axioms = self._load_tbox_axioms()
        self._inverse_object_property_pairs = self._load_inverse_object_property_pairs()
        self.base_ontology = SyncOntology(IRI.create("file:/base.owl"), load=False)
        for ax in self._tbox_axioms:
            self.base_ontology.add_axiom(ax)
        self.inferred_ontology = None
        self.CORE_CONTEXT = self._safe_read(self.CORE_PATH)
        self.record_idx = self.payload.get("idx", record.idx if hasattr(record, "idx") else "demo")
        self.ontology_context = self.CORE_CONTEXT
        self.max_iters = max_iters
        self.prompt_cache_enabled = self.llm_provider == "openai" and os.getenv("OPENAI_ENABLE_PROMPT_CACHING", "1").strip().lower() not in {"0", "false", "no", "off"}
        requested_retention = os.getenv("OPENAI_PROMPT_CACHE_RETENTION", "24h").strip() or "24h"
        self.prompt_cache_retention = requested_retention if requested_retention in {"in_memory", "24h"} else "24h"
        self.prompt_cache_namespace = os.getenv("OPENAI_PROMPT_CACHE_NAMESPACE", "req-iof")
        self.last_prompt_cache_usage: Optional[Dict[str, Any]] = None
        self.iof_ns = "https://spec.industrialontologies.org/ontology/core/Core/"
        self.qudt_ns = "http://qudt.org/schema/qudt/"
        self.qudt_qk_ns = "http://qudt.org/vocab/quantitykind/"
        self.qudt_unit_ns = "http://qudt.org/vocab/unit/"

        self.value_expression_class = OWLClass(IRI(self.iof_ns, "ValueExpression"))
        self.quantity_value_class = OWLClass(IRI(self.qudt_ns, "QuantityValue"))
        self.unit_class = OWLClass(IRI(self.qudt_ns, "Unit"))

        self.qudt_unit_op = OWLObjectProperty(IRI(self.qudt_ns, "unit"))
        self.qudt_has_unit_op = OWLObjectProperty(IRI(self.qudt_ns, "hasUnit"))
        self.qudt_has_quantity_kind_op = OWLObjectProperty(IRI(self.qudt_ns, "hasQuantityKind"))
        self.has_value_expr_some_time = OWLObjectProperty(IRI(self.iof_ns, "hasValueExpressionAtSomeTime"))
        self.has_value_expr_all_times = OWLObjectProperty(IRI(self.iof_ns, "hasValueExpressionAtAllTimes"))
        self.is_value_expr_of_some_time = OWLObjectProperty(IRI(self.iof_ns, "isValueExpressionOfAtSomeTime"))
        self.is_value_expr_of_all_times = OWLObjectProperty(IRI(self.iof_ns, "isValueExpressionOfAtAllTimes"))
        self.has_measured_value_some_time = OWLObjectProperty(IRI(self.iof_ns, "hasMeasuredValueAtSomeTime"))
        self.has_measured_value_all_times = OWLObjectProperty(IRI(self.iof_ns, "hasMeasuredValueAtAllTimes"))
        self.is_measured_value_of_some_time = OWLObjectProperty(IRI(self.iof_ns, "isMeasuredValueOfAtSomeTime"))
        self.is_measured_value_of_all_times = OWLObjectProperty(IRI(self.iof_ns, "isMeasuredValueOfAtAllTimes"))

        self.qudt_numeric_value_dp = OWLDataProperty(IRI(self.qudt_ns, "numericValue"))
        self.qudt_lower_bound_dp = OWLDataProperty(IRI(self.qudt_ns, "lowerBound"))
        self.qudt_upper_bound_dp = OWLDataProperty(IRI(self.qudt_ns, "upperBound"))
        self.qudt_min_inclusive_dp = OWLDataProperty(IRI(self.qudt_ns, "minInclusive"))
        self.qudt_max_inclusive_dp = OWLDataProperty(IRI(self.qudt_ns, "maxInclusive"))
        self.qudt_min_exclusive_dp = OWLDataProperty(IRI(self.qudt_ns, "minExclusive"))
        self.qudt_max_exclusive_dp = OWLDataProperty(IRI(self.qudt_ns, "maxExclusive"))
        self.qudt_lower_bound_inclusive_dp = OWLDataProperty(IRI(self.qudt_ns, "lowerBoundInclusive"))
        self.qudt_upper_bound_inclusive_dp = OWLDataProperty(IRI(self.qudt_ns, "upperBoundInclusive"))
        self.SYSTEM_PROMPT_TEMPLATE = dedent(
        """
        You are an ontological engineer. Given a structured extraction of semantic entities from a requirement sentence, produce an OWL graph in Turtle (.ttl) format.

        Inputs you must use directly:
        - Structured extraction + alignment payload (JSON below), including `normalized_quantities` entries from the NormalizedRecord.
        - Full IOF Core ontology (Core.rdf, RDF/XML) as authoritative vocabulary and axioms.

        Goals (in order):
        1) Map the whole requirement into IOF classes and properties. Do not define new classes or properties of any kind.
        2) Avoid blank nodes; mint readable IRIs under the base.
        3) Only for true quantitative constraints, create a value individual typed iof:ValueExpression.
        4) Reuse individuals rather than duplicating (e.g., the same quality/agent that the value qualifies).
        5) Keep the graph OWL DL compatible and minimal.

        Graph pattern constraints (mandatory):

        - Create exactly one iof:RequirementSpecification individual :Req_0 and attach a comment to it with the original_text of the input requirement.
        - The requirement specification individual must be connected to a specification instance via iof:requirementSatisfiedBy.
        - Use iof:DesignSpecification when the requirement constrains continuants (artifacts/material entities/qualities of continuants).
        - Use iof:PlanSpecification when the requirement constrains processes, events, procedures, or process characteristics.
        - Do not use QUDT classes or properties. They will be handled in a later enrichment step. Once you instantiate a iof:ValueExpression, that must be a leaf node with no further QUDT properties.
        - Use iof:ValueExpression class for value individuals. Do not use its subclasses (such as iof:MeasuredValueExpression).
        - Connect each quantitative ValueExpression to its constrained bearer using iof:isValueExpressionOfAtSomeTime / iof:isValueExpressionOfAtAllTimes (or the inverse hasValueExpression* relation from the bearer).
        - iof:hasSpecifiedOutput has domain iof:PlannedProcess. Never use iof:hasSpecifiedOutput on any specification individual (DesignSpecification / PlanSpecification / RequirementSpecification / ObjectiveSpecification / InformationContentEntity). Use iof:prescribes for specification semantics.
        - A iof:DesignSpecification MUST NOT prescribe occurrents. In particular, never assert:
          - DesignSpecification iof:prescribes Process
          - DesignSpecification iof:prescribes PlannedProcess
          - DesignSpecification iof:prescribes ProcessCharacteristic
          - DesignSpecification iof:prescribes Event
        - For every quantitative constraint, create exactly one value node with stable naming `:VE_req<req_idx>_c<constraint_idx>`. Do not create additional quantitative ValueExpression nodes for the same constraint.
        - Never create iof:ValueExpression for relation, enum, boolean, event_ref, entity_ref, or textual-conformance constraints.
        - Only create iof:ValueExpression when the payload has matching evidence:
          - `constraints[*].value.kind == "quantity"` and
          - a corresponding `normalized_quantities` entry for the same `(req_idx, constraint_idx)`.
        - If there are zero quantitative constraints / zero `normalized_quantities` rows, output MUST contain zero `:VE_req*` individuals and zero `iof:ValueExpression` instances.
        - If a specification prescribes any process-like entity, type it as iof:PlanSpecification (not iof:DesignSpecification).
        - If a value-expression node has QUDT numeric/bound data properties, do not assert iof:hasSimpleExpressionValue on that same node.

        Mandatory prefixes (always include):
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix iof: <https://spec.industrialontologies.org/ontology/core/Core/> .
        @prefix bfo: <http://purl.obolibrary.org/obo/> .
        @prefix qudt: <http://qudt.org/schema/qudt/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix : <http://example.org/req/{record_idx}#> .

        Authoritative ontology (full Core.rdf content):
        {ontology_context}

        Return ONLY OWL/Turtle (TTL syntax), no commentary, no RDF/XML, base IRI http://example.org/req/{record_idx}#
        Before final output, verify each req_idx and each constraint_idx is represented in triples.
        """
    )
        self.ZERO_SHOT_IOF_QUDT_RULES = dedent(
        """
        IOF + QUDT quantitative pattern (mandatory when a true quantitative constraint exists):
        - Create exactly one value node per quantitative constraint with stable naming `:VE_req<req_idx>_c<constraint_idx>`.
        - Type each such node as both `iof:ValueExpression` and `qudt:QuantityValue`.
        - Link the constrained bearer to the value node using `iof:isValueExpressionOfAtSomeTime` / `iof:isValueExpressionOfAtAllTimes`
          (or the inverse `iof:hasValueExpressionAtSomeTime` / `iof:hasValueExpressionAtAllTimes`).
        - Attach the unit to the value node with `qudt:unit`.
        - Put `qudt:hasQuantityKind` on the unit individual, never on the value node.
        - Type used units as `qudt:Unit` and used quantity kinds as `qudt:QuantityKind`.
        - For exact values, use `qudt:numericValue`.
        - For bounded values, use only:
          - `qudt:minInclusive` / `qudt:maxInclusive`
          - `qudt:minExclusive` / `qudt:maxExclusive`
          - `qudt:lowerBound` / `qudt:upperBound` only when inclusivity is unknown
        - Never use `qudt:lowerBoundInclusive` or `qudt:upperBoundInclusive`.
        - If QUDT numeric or bound properties are present on a value node, do not assert `iof:hasSimpleExpressionValue` on that same node.
        - Never create quantitative nodes for relation, enum, boolean, event_ref, entity_ref, or purely textual constraints.
        """
    )
        self.ZERO_SHOT_SYSTEM_PROMPT_TEMPLATE = dedent(
        """
        You are an ontological engineer. Given a structured extraction of semantic entities from a requirement sentence, produce an OWL graph in Turtle (.ttl) format.

        Inputs you must use directly:
        - Structured extraction + alignment payload (JSON below), including `normalized_quantities` entries from the NormalizedRecord.
        - Full IOF Core ontology (Core.rdf, RDF/XML) as authoritative vocabulary and axioms.

        Goals (in order):
        1) Map the whole requirement into IOF classes and properties. Do not define new classes or properties of any kind.
        2) Avoid blank nodes; mint readable IRIs under the base.
        3) For true quantitative constraints, emit the IOF + QUDT pattern directly in this single pass.
        4) Reuse individuals rather than duplicating (e.g., the same quality/agent that the value qualifies).
        5) Keep the graph OWL DL compatible and minimal.

        Graph pattern constraints (mandatory):

        - Create exactly one iof:RequirementSpecification individual :Req_0 and attach a comment to it with the original_text of the input requirement.
        - The requirement specification individual must be connected to a specification instance via iof:requirementSatisfiedBy.
        - Use iof:DesignSpecification when the requirement constrains continuants (artifacts/material entities/qualities of continuants).
        - Use iof:PlanSpecification when the requirement constrains processes, events, procedures, or process characteristics.
        - iof:hasSpecifiedOutput has domain iof:PlannedProcess. Never use iof:hasSpecifiedOutput on any specification individual (DesignSpecification / PlanSpecification / RequirementSpecification / ObjectiveSpecification / InformationContentEntity). Use iof:prescribes for specification semantics.
        - A iof:DesignSpecification MUST NOT prescribe occurrents. In particular, never assert:
          - DesignSpecification iof:prescribes Process
          - DesignSpecification iof:prescribes PlannedProcess
          - DesignSpecification iof:prescribes ProcessCharacteristic
          - DesignSpecification iof:prescribes Event
        - If a specification prescribes any process-like entity, type it as iof:PlanSpecification (not iof:DesignSpecification).
        - Use `normalized_quantities` as authoritative for unit URI, quantity kind URI, and numeric bounds whenever those rows exist for a constraint.
        {zero_shot_iof_qudt_rules}

        Mandatory prefixes (always include):
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix iof: <https://spec.industrialontologies.org/ontology/core/Core/> .
        @prefix bfo: <http://purl.obolibrary.org/obo/> .
        @prefix qudt: <http://qudt.org/schema/qudt/> .
        @prefix qudtu: <http://qudt.org/vocab/unit/> .
        @prefix qudtqk: <http://qudt.org/vocab/quantitykind/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix : <http://example.org/req/{record_idx}#> .

        Authoritative ontology (full Core.rdf content):
        {ontology_context}

        Return ONLY OWL/Turtle (TTL syntax), no commentary, no RDF/XML, base IRI http://example.org/req/{record_idx}#
        Before final output, verify each req_idx and each constraint_idx in the payload is represented in triples.
        """
    )
        
    def _build_prompt_cache_key(self, scope: str, model: str) -> str:
        """Build a stable cache key for prompt families that share the same large prefix."""
        ontology_digest = hashlib.sha256(self.CORE_CONTEXT.encode("utf-8")).hexdigest()[:16]
        return f"{self.prompt_cache_namespace}:{scope}:{model}:{ontology_digest}"

    def _record_prompt_cache_usage(self, completion: Any) -> None:
        usage = getattr(completion, "usage", None)
        prompt_details = getattr(usage, "prompt_tokens_details", None) if usage is not None else None
        self.last_prompt_cache_usage = {
            "prompt_tokens": getattr(usage, "prompt_tokens", None),
            "completion_tokens": getattr(usage, "completion_tokens", None),
            "total_tokens": getattr(usage, "total_tokens", None),
            "cached_tokens": getattr(prompt_details, "cached_tokens", None),
        }

    def _chat_completion_create(
        self,
        *,
        model: str,
        temperature: float,
        messages: List[Dict[str, Any]],
        cache_scope: Optional[str] = None,
        client: Optional[OpenAI] = None,
    ) -> Any:
        request_kwargs: Dict[str, Any] = {
            "model": model,
            "temperature": temperature,
            "messages": messages,
        }
        if self.prompt_cache_enabled and cache_scope:
            request_kwargs["prompt_cache_key"] = self._build_prompt_cache_key(cache_scope, model)
            request_kwargs["prompt_cache_retention"] = self.prompt_cache_retention

        completion = (client or self.client).chat.completions.create(**request_kwargs)
        self._record_prompt_cache_usage(completion)
        return completion

    def llm_repair_graph(
        self,
        inconsistent_owl: str,
        report: str,
        model: Optional[str] = None,
        temperature: float = 0,
    ) -> str:
        selected_model = model or self.model
        system_prompt = "You repair OWL graphs to satisfy DL reasoners while keeping to the full IOF mapping goals."
        prompt = f"""
    The previous owl graph was inconsistent or incomplete. Fix it and output corrected OWL/Turtle only.

    Rules (must follow):
    - Keep base IRI and identifiers.
    - Do not invent new classes/properties; only IOF/BFO IRIs provided in the context.
    - Represent the full requirement (subject, modality, condition, action, object, constraints, references) in IOF/BFO.
    - Prefer tightening types/domains to restore consistency over dropping constraints.
    - Do not use iof:hasSpecifiedOutput on specification individuals; iof:hasSpecifiedOutput is only for iof:PlannedProcess subjects.
    - If a value node has QUDT numeric/bound data properties, do not keep iof:hasSimpleExpressionValue on that same node.

    Ontology context (Core.rdf):
    {self.ontology_context}

    Inputs:
    - Structured payload (authoritative):
    {json.dumps(self.payload, indent=2)}
    - Previous graph:
    {inconsistent_owl}
    - Reasoner report:
    {report}

    Output constraints:
    - Return Turtle syntax only.
    - Do not return RDF/XML.
    - Do not include XML declarations (for example: <?xml ...?>).
        """
        completion = self._chat_completion_create(
            model=selected_model,
            temperature=temperature,
            cache_scope="repair-structured",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
        )
        return self._coerce_to_turtle_text(completion.choices[0].message.content)

    def llm_repair_graph_from_raw_requirement(
        self,
        inconsistent_owl: str,
        report: str,
        model: Optional[str] = None,
        temperature: float = 0,
    ) -> str:
        selected_model = model or self.model
        original_text = str(self.payload.get("original_text") or "").strip()
        system_prompt = "You repair OWL graphs to satisfy DL reasoners while keeping to the full IOF + QUDT mapping goals."
        prompt = f"""
    The previous OWL graph was inconsistent or incomplete. Fix it and output corrected OWL/Turtle only.

    Rules (must follow):
    - Keep base IRI and identifiers.
    - Do not invent new classes/properties; only IOF/BFO/QUDT IRIs provided in the context.
    - Represent the requirement faithfully from the raw sentence only; do not assume hidden extraction or normalization artifacts.
    - Preserve the IOF + QUDT quantitative pattern when a true quantitative constraint exists.
    - Prefer tightening types/domains to restore consistency over dropping constraints.
    - Do not use iof:hasSpecifiedOutput on specification individuals; iof:hasSpecifiedOutput is only for iof:PlannedProcess subjects.
    - If a value node has QUDT numeric/bound data properties, do not keep iof:hasSimpleExpressionValue on that same node.
    - Keep exactly one quantitative value node per quantitative constraint.
    - Do not place qudt:hasQuantityKind on the value node; keep it on the unit individual.

    Raw requirement sentence:
    {original_text}

    Ontology context (Core.rdf):
    {self.ontology_context}

    Previous graph:
    {inconsistent_owl}

    Reasoner report:
    {report}

    Output constraints:
    - Return Turtle syntax only.
    - Do not return RDF/XML.
    - Do not include XML declarations.
        """
        completion = self._chat_completion_create(
            model=selected_model,
            temperature=temperature,
            cache_scope="repair-raw",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
        )
        return self._coerce_to_turtle_text(completion.choices[0].message.content)

    def two_stage_workflow(self) -> Dict[str, Any]:
        """Run LLM stage then deterministic QUDT enrichment with LLM fallback only if needed."""
        stage1 = self.agentic_loop()
        initial_owl = self.ensure_prefixes(stage1["owl"])
        has_quant_rows = bool(self.flatten_normalization(self.payload))

        final_owl, rule_actions = self.apply_qudt_from_normalization(initial_owl, self.payload)
        final_owl, spec_output_actions = self._sanitize_specification_has_specified_output_usage(final_owl)
        final_owl, structural_actions = self._sanitize_design_specification_usage(final_owl)
        final_owl, value_literal_actions = self._sanitize_qudt_valueexpression_literals(final_owl)
        rule_actions.extend(spec_output_actions)
        rule_actions.extend(structural_actions)
        rule_actions.extend(value_literal_actions)
        violations = self._validate_iof_qudt_pattern(final_owl, payload=self.payload)
        if violations and has_quant_rows:
            repaired_owl, llm_actions = self.llm_apply_qudt_guideline(final_owl, self.payload)
            final_owl, repair_actions = self.apply_qudt_from_normalization(repaired_owl, self.payload)
            final_owl, post_spec_output_actions = self._sanitize_specification_has_specified_output_usage(final_owl)
            final_owl, post_structural_actions = self._sanitize_design_specification_usage(final_owl)
            final_owl, post_value_literal_actions = self._sanitize_qudt_valueexpression_literals(final_owl)
            post_violations = self._validate_iof_qudt_pattern(final_owl, payload=self.payload)
            rule_actions.extend(llm_actions)
            rule_actions.extend(repair_actions)
            rule_actions.extend(post_spec_output_actions)
            rule_actions.extend(post_structural_actions)
            rule_actions.extend(post_value_literal_actions)
            if post_violations:
                rule_actions.append(
                    {
                        "note": "Validation violations remain after deterministic+LLM repair.",
                        "violations": post_violations,
                    }
                )
            else:
                rule_actions.append({"note": "Validation passed after deterministic+LLM repair."})
        elif violations and not has_quant_rows:
            rule_actions.append(
                {
                    "note": "Skipped LLM QUDT enrichment because no quantitative rows exist.",
                    "violations": violations,
                }
            )
        else:
            rule_actions.append({"note": "Validation passed after deterministic enrichment."})

        output_paths = self.save_workflow_outputs(initial_owl=initial_owl, final_owl=final_owl)
        return {
            "stage1": stage1,
            "initial_owl": initial_owl,
            "final_owl": final_owl,
            "rule_actions": rule_actions,
            "output_paths": output_paths,
        }

    def zero_shot_workflow(self) -> Dict[str, Any]:
        """Single-pass LLM grounding with no repair loop or deterministic enrichment."""
        owl = self.llm_build_graph_zero_shot(self.client, self.payload)
        owl = self.ensure_prefixes(owl)
        base = f"http://example.org/req/{self.payload.get('idx', 'demo')}"
        owl = self.enforce_base(owl, base)
        owl = self.add_ontology_header(owl, base if base.endswith(('#', '/')) else base + '#')
        return {
            "stage1": {
                "history": [],
                "mode": "zero_shot_llm",
            },
            "initial_owl": owl,
            "final_owl": owl,
            "rule_actions": [{"note": "Zero-shot grounding: single LLM pass with inline IOF+QUDT grounding and no repair or deterministic enrichment."}],
            "output_paths": {},
        }

    def raw_zero_shot_workflow(self) -> Dict[str, Any]:
        """Single-pass LLM grounding directly from the raw requirement text."""
        owl = self.llm_build_graph_from_raw_requirement(self.client, self.payload)
        owl = self.ensure_prefixes(owl)
        base = f"http://example.org/req/{self.payload.get('idx', 'demo')}"
        owl = self.enforce_base(owl, base)
        owl = self.add_ontology_header(owl, base if base.endswith(('#', '/')) else base + '#')
        return {
            "stage1": {
                "history": [],
                "mode": "raw_zero_shot_llm",
            },
            "initial_owl": owl,
            "final_owl": owl,
            "rule_actions": [{"note": "Raw zero-shot grounding: direct single LLM pass from original_text with inline IOF+QUDT grounding and no repair."}],
            "output_paths": {},
        }

    def raw_agentic_workflow(self) -> Dict[str, Any]:
        """Raw-input grounding with the same repair loop as agentic grounding, but no normalization-driven enrichment."""
        stage1 = self.raw_agentic_loop()
        final_owl = self.ensure_prefixes(stage1["owl"])
        return {
            "stage1": stage1,
            "initial_owl": stage1["initial_owl"],
            "final_owl": final_owl,
            "rule_actions": [{"note": "Raw agentic grounding: raw IOF+QUDT prompt with reasoner-feedback repair loop and no deterministic normalization-based enrichment."}],
            "output_paths": {},
        }

    def _entity_str(self, entity: Any) -> str:
        if hasattr(entity, "str"):
            return entity.str
        iri = getattr(entity, "iri", None)
        if iri is not None and hasattr(iri, "str"):
            return iri.str
        return str(entity)

    def _split_uri(self, uri: str) -> Tuple[str, str]:
        if "#" in uri:
            ns, local = uri.rsplit("#", 1)
            return f"{ns}#", local
        ns, local = uri.rsplit("/", 1)
        return f"{ns}/", local

    def _individual_from_uri(self, uri: str) -> OWLNamedIndividual:
        ns, local = self._split_uri(uri)
        return OWLNamedIndividual(IRI(ns, local))

    def _class_from_uri(self, uri: str) -> OWLClass:
        ns, local = self._split_uri(uri)
        return OWLClass(IRI(ns, local))

    def _object_property_from_uri(self, uri: str) -> OWLObjectProperty:
        ns, local = self._split_uri(uri)
        return OWLObjectProperty(IRI(ns, local))

    def _data_property_from_uri(self, uri: str) -> OWLDataProperty:
        ns, local = self._split_uri(uri)
        return OWLDataProperty(IRI(ns, local))

    def _owl_literal_from_rdflib_literal(self, literal: Literal) -> OWLLiteral:
        text = str(literal)
        datatype = literal.datatype
        try:
            if datatype in {
                XSD.integer,
                XSD.int,
                XSD.long,
                XSD.short,
                XSD.byte,
                XSD.nonNegativeInteger,
                XSD.nonPositiveInteger,
                XSD.positiveInteger,
                XSD.negativeInteger,
            }:
                return OWLLiteral(int(text))
            if datatype in {XSD.decimal, XSD.double, XSD.float}:
                return OWLLiteral(float(text))
            if datatype == XSD.boolean:
                return OWLLiteral(text.strip().lower() in {"true", "1"})
        except Exception:
            pass
        return OWLLiteral(text)

    def _canonical_unit_uri(self, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = str(value).strip()
        if not value:
            return None
        if value.startswith("http://qudt.org/vocab/unit/"):
            return value
        if value.startswith("http://") or value.startswith("https://"):
            return value
        return f"{self.qudt_unit_ns}{value}"

    def _canonical_qk_uri(self, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = str(value).strip()
        if not value:
            return None
        if value.startswith("http://qudt.org/vocab/quantitykind/"):
            return value
        if value.startswith("http://") or value.startswith("https://"):
            return value
        return f"{self.qudt_qk_ns}{value}"

    def flatten_normalization(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        requirements = payload.get("requirements", [])
        for req in requirements:
            req_idx = req.get("req_idx")
            constraints = req.get("constraints", [])
            for nq in req.get("normalized_quantities", []):
                constraint_idx = nq.get("constraint_idx")
                constraint = None
                if isinstance(constraint_idx, int) and 0 <= constraint_idx < len(constraints):
                    constraint = constraints[constraint_idx]
                attribute_name = None
                operator = None
                if constraint:
                    attribute = constraint.get("attribute", {})
                    attribute_name = attribute.get("name")
                    operator = constraint.get("operator")

                unit_uri = self._canonical_unit_uri(nq.get("si_unit_primary") or nq.get("best_unit_uri"))
                qk_uri = self._canonical_qk_uri(nq.get("quantity_kind_uri"))

                rows.append(
                    {
                        "req_idx": req_idx,
                        "constraint_idx": constraint_idx,
                        "attribute_name": attribute_name,
                        "operator": operator,
                        "unit_uri": unit_uri,
                        "quantity_kind_uri": qk_uri,
                        "si_value_primary": nq.get("si_value_primary"),
                        "si_value_secondary": nq.get("si_value_secondary"),
                        "lower_bound": nq.get("lower_bound"),
                        "upper_bound": nq.get("upper_bound"),
                        "lower_bound_included": nq.get("lower_bound_included"),
                        "upper_bound_included": nq.get("upper_bound_included"),
                    }
                )

        rows.sort(key=lambda r: (r.get("req_idx", 0), r.get("constraint_idx", 0)))
        return rows

    def _value_node_uri(self, req_idx: int, constraint_idx: int) -> URIRef:
        return URIRef(f"http://example.org/req/{self.payload.get('idx', 'demo')}#VE_req{req_idx}_c{constraint_idx}")

    def _value_nodes_with_row_mapping(
        self, graph: rdflib.Graph, rows: List[Dict[str, Any]]
    ) -> Tuple[Dict[URIRef, Dict[str, Any]], List[Dict[str, Any]]]:
        row_by_key: Dict[Tuple[int, int], Dict[str, Any]] = {}
        for row in rows:
            req_idx = row.get("req_idx")
            constraint_idx = row.get("constraint_idx")
            if isinstance(req_idx, int) and isinstance(constraint_idx, int):
                row_by_key[(req_idx, constraint_idx)] = row

        iof_value_expr = URIRef(f"{self.iof_ns}ValueExpression")
        qudt_quantity_value = URIRef(f"{self.qudt_ns}QuantityValue")
        existing_nodes = set(graph.subjects(RDF.type, iof_value_expr)) | set(graph.subjects(RDF.type, qudt_quantity_value))

        assignments: Dict[URIRef, Dict[str, Any]] = {}
        used_keys: set[Tuple[int, int]] = set()
        actions: List[Dict[str, Any]] = []

        for node in sorted(existing_nodes, key=lambda n: str(n)):
            local_name = self._get_local_name(str(node))
            match = re.match(r"VE_req(\d+)_c(\d+)$", local_name)
            if not match:
                continue
            key = (int(match.group(1)), int(match.group(2)))
            row = row_by_key.get(key)
            if row is None or key in used_keys:
                continue
            assignments[node] = row
            used_keys.add(key)
            actions.append({"node": str(node), "mapping": "canonical_name_match", "key": key})

        for key, row in sorted(row_by_key.items(), key=lambda item: item[0]):
            if key in used_keys:
                continue
            node = self._value_node_uri(key[0], key[1])
            assignments[node] = row
            used_keys.add(key)
            actions.append({"node": str(node), "mapping": "created_canonical_node", "key": key})

        return assignments, actions

    def _clear_value_node_numeric_assertions(self, graph: rdflib.Graph, node: URIRef) -> None:
        predicates = [
            URIRef(f"{self.qudt_ns}numericValue"),
            URIRef(f"{self.qudt_ns}lowerBound"),
            URIRef(f"{self.qudt_ns}upperBound"),
            URIRef(f"{self.qudt_ns}minInclusive"),
            URIRef(f"{self.qudt_ns}maxInclusive"),
            URIRef(f"{self.qudt_ns}minExclusive"),
            URIRef(f"{self.qudt_ns}maxExclusive"),
            URIRef(f"{self.qudt_ns}lowerBoundInclusive"),
            URIRef(f"{self.qudt_ns}upperBoundInclusive"),
        ]
        for predicate in predicates:
            for obj in list(graph.objects(node, predicate)):
                graph.remove((node, predicate, obj))

    def _apply_bounds_or_numeric(
        self, graph: rdflib.Graph, node: URIRef, row: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        actions: List[Dict[str, Any]] = []
        p_numeric = URIRef(f"{self.qudt_ns}numericValue")
        p_lower = URIRef(f"{self.qudt_ns}lowerBound")
        p_upper = URIRef(f"{self.qudt_ns}upperBound")
        p_min_inc = URIRef(f"{self.qudt_ns}minInclusive")
        p_max_inc = URIRef(f"{self.qudt_ns}maxInclusive")
        p_min_exc = URIRef(f"{self.qudt_ns}minExclusive")
        p_max_exc = URIRef(f"{self.qudt_ns}maxExclusive")

        lower = row.get("lower_bound")
        upper = row.get("upper_bound")
        lower_inc = self._coerce_optional_bool(row.get("lower_bound_included"))
        upper_inc = self._coerce_optional_bool(row.get("upper_bound_included"))

        if lower is not None:
            lower_literal = Literal(float(lower), datatype=XSD.double)
            if lower_inc is True:
                graph.add((node, p_min_inc, lower_literal))
                actions.append({"node": str(node), "added": "qudt:minInclusive", "value": float(lower)})
            elif lower_inc is False:
                graph.add((node, p_min_exc, lower_literal))
                actions.append({"node": str(node), "added": "qudt:minExclusive", "value": float(lower)})
            else:
                graph.add((node, p_lower, lower_literal))
                actions.append({"node": str(node), "added": "qudt:lowerBound", "value": float(lower)})

        if upper is not None:
            upper_literal = Literal(float(upper), datatype=XSD.double)
            if upper_inc is True:
                graph.add((node, p_max_inc, upper_literal))
                actions.append({"node": str(node), "added": "qudt:maxInclusive", "value": float(upper)})
            elif upper_inc is False:
                graph.add((node, p_max_exc, upper_literal))
                actions.append({"node": str(node), "added": "qudt:maxExclusive", "value": float(upper)})
            else:
                graph.add((node, p_upper, upper_literal))
                actions.append({"node": str(node), "added": "qudt:upperBound", "value": float(upper)})

        if lower is None and upper is None and row.get("si_value_primary") is not None:
            graph.add((node, p_numeric, Literal(float(row["si_value_primary"]), datatype=XSD.double)))
            actions.append({"node": str(node), "added": "qudt:numericValue", "value": float(row["si_value_primary"])})

        return actions

    def apply_qudt_from_normalization(
        self,
        owl_text: str,
        payload: Dict[str, Any],
    ) -> Tuple[str, List[Dict[str, Any]]]:
        graph = self._parse_graph_from_text(owl_text)
        graph.bind("iof", URIRef(self.iof_ns))
        graph.bind("qudt", URIRef(self.qudt_ns))
        graph.bind("qudtu", URIRef(self.qudt_unit_ns))
        graph.bind("qudtqk", URIRef(self.qudt_qk_ns))
        graph.bind("xsd", URIRef(str(XSD)))

        iof_value_expr = URIRef(f"{self.iof_ns}ValueExpression")
        qudt_quantity_value = URIRef(f"{self.qudt_ns}QuantityValue")
        qudt_unit_cls = URIRef(f"{self.qudt_ns}Unit")
        qudt_qk_cls = URIRef(f"{self.qudt_ns}QuantityKind")
        p_unit = URIRef(f"{self.qudt_ns}unit")
        p_has_qk = URIRef(f"{self.qudt_ns}hasQuantityKind")

        rows = self.flatten_normalization(payload)
        assignments, actions = self._value_nodes_with_row_mapping(graph, rows)
        if not assignments:
            # Deterministically strip any accidental quantitative modeling when normalization has no rows.
            # This prevents the later LLM QUDT step from being triggered for non-quantitative requirements.
            value_nodes = set(graph.subjects(RDF.type, iof_value_expr)) | set(graph.subjects(RDF.type, qudt_quantity_value))
            removed_value_node_triples = 0
            for node in list(value_nodes):
                outgoing = list(graph.triples((node, None, None)))
                incoming = list(graph.triples((None, None, node)))
                removed_value_node_triples += len(outgoing) + len(incoming)
                for triple in outgoing:
                    graph.remove(triple)
                for triple in incoming:
                    graph.remove(triple)

            iof_value_predicates = [
                URIRef(f"{self.iof_ns}hasValueExpressionAtSomeTime"),
                URIRef(f"{self.iof_ns}hasValueExpressionAtAllTimes"),
                URIRef(f"{self.iof_ns}isValueExpressionOfAtSomeTime"),
                URIRef(f"{self.iof_ns}isValueExpressionOfAtAllTimes"),
                URIRef(f"{self.iof_ns}hasMeasuredValueAtSomeTime"),
                URIRef(f"{self.iof_ns}hasMeasuredValueAtAllTimes"),
                URIRef(f"{self.iof_ns}isMeasuredValueOfAtSomeTime"),
                URIRef(f"{self.iof_ns}isMeasuredValueOfAtAllTimes"),
            ]
            removed_iof_value_predicate_triples = 0
            for predicate in iof_value_predicates:
                triples = list(graph.triples((None, predicate, None)))
                removed_iof_value_predicate_triples += len(triples)
                for triple in triples:
                    graph.remove(triple)

            qudt_predicate_triples = list(
                graph.triples((None, None, None))
            )
            removed_qudt_predicate_triples = 0
            for s, p, o in qudt_predicate_triples:
                if str(p).startswith(self.qudt_ns):
                    graph.remove((s, p, o))
                    removed_qudt_predicate_triples += 1

            actions.append(
                {
                    "note": "No quantitative rows found; removed accidental quantitative artifacts.",
                    "removed_value_node_triples": removed_value_node_triples,
                    "removed_iof_value_predicate_triples": removed_iof_value_predicate_triples,
                    "removed_qudt_predicate_triples": removed_qudt_predicate_triples,
                }
            )
            ttl = graph.serialize(format="turtle")
            if isinstance(ttl, bytes):
                ttl = ttl.decode("utf-8")
            return self.ensure_prefixes(self._normalize_qudt_prefix_aliases(ttl)), actions

        for node, row in assignments.items():
            graph.add((node, RDF.type, iof_value_expr))
            graph.add((node, RDF.type, qudt_quantity_value))

            for bad_qk in list(graph.objects(node, p_has_qk)):
                graph.remove((node, p_has_qk, bad_qk))

            unit_uri = self._canonical_unit_uri(row.get("unit_uri"))
            if unit_uri:
                unit = URIRef(unit_uri)
                for old_unit in list(graph.objects(node, p_unit)):
                    if old_unit != unit:
                        graph.remove((node, p_unit, old_unit))
                graph.add((node, p_unit, unit))
                graph.add((unit, RDF.type, qudt_unit_cls))
                actions.append({"node": str(node), "added": "qudt:unit", "unit": str(unit)})

                qk_uri = self._canonical_qk_uri(row.get("quantity_kind_uri"))
                if qk_uri:
                    qk = URIRef(qk_uri)
                    graph.add((unit, p_has_qk, qk))
                    graph.add((qk, RDF.type, qudt_qk_cls))
                    actions.append({"unit": str(unit), "added": "qudt:hasQuantityKind", "quantity_kind": str(qk)})

            self._clear_value_node_numeric_assertions(graph, node)
            actions.extend(self._apply_bounds_or_numeric(graph, node, row))

        ttl = graph.serialize(format="turtle")
        if isinstance(ttl, bytes):
            ttl = ttl.decode("utf-8")
        return self.ensure_prefixes(self._normalize_qudt_prefix_aliases(ttl)), actions

    def save_workflow_outputs(self, initial_owl: str, final_owl: str, output_dir: Optional[Path] = None) -> Dict[str, str]:
        idx = self.payload.get("idx", "demo")
        out_dir = output_dir or (Path(__file__).resolve().parents[1] / "outputs")
        out_dir.mkdir(parents=True, exist_ok=True)

        initial_path = out_dir / "initial_KG.ttl"
        final_path = out_dir / f"final_kg_{idx}.ttl"

        initial_path.write_text(self.ensure_prefixes(initial_owl), encoding="utf-8")
        final_path.write_text(self.ensure_prefixes(final_owl), encoding="utf-8")

        return {
            "initial_owl": str(initial_path.resolve()),
            "final_owl": str(final_path.resolve()),
        }

    def build_normalized_record_brief(self, payload: Dict[str, Any], max_rows: int = 50) -> str:
        rows = self.flatten_normalization(payload)
        if not rows:
            return "No normalized_quantities were provided in the payload."

        lines: List[str] = []
        for row in rows[:max_rows]:
            lower = row.get("lower_bound")
            upper = row.get("upper_bound")
            lb_inc = row.get("lower_bound_included")
            ub_inc = row.get("upper_bound_included")
            bounds = (
                f"lower={lower} (included={lb_inc}), "
                f"upper={upper} (included={ub_inc})"
            )
            lines.append(
                (
                    f"- req_idx={row.get('req_idx')}, constraint_idx={row.get('constraint_idx')}, "
                    f"attribute={row.get('attribute_name')}, operator={row.get('operator')}, "
                    f"si_value_primary={row.get('si_value_primary')}, "
                    f"unit_uri={row.get('unit_uri')}, qk_uri={row.get('quantity_kind_uri')}, {bounds}"
                )
            )
        if len(rows) > max_rows:
            lines.append(f"- ... truncated {len(rows) - max_rows} additional normalized rows")
        return "\n".join(lines)

    def _load_ontology_from_text(self, owl_text: str) -> Tuple[Ontology, Path]:
        with NamedTemporaryFile("w", suffix=".ttl", encoding="utf-8", delete=False) as tmp:
            tmp.write(owl_text)
            tmp_path = Path(tmp.name)
        # Use OWLAPI-backed ontology loading here so get_abox_axioms() is consistent
        # with what the reasoner sees in SyncOntology.
        onto = SyncOntology(str(tmp_path))
        return onto, tmp_path

    def _update_base_ontology_from_owl(self, owl_text: str) -> None:
        parsed_onto, tmp_path = self._load_ontology_from_text(owl_text)
        try:
            self.base_ontology = SyncOntology(IRI.create("file:/base.owl"), load=False)
            for ax in self._tbox_axioms:
                self.base_ontology.add_axiom(ax)
            for ax in parsed_onto.get_abox_axioms():
                self.base_ontology.add_axiom(ax)

            # Ensure ABox assertion coverage by directly projecting RDF triples.
            # This captures object/data property assertions even when the ontology
            # loader exposes only a subset via get_abox_axioms().
            graph = self._parse_graph_from_text(owl_text)
            for subject, predicate, obj in graph:
                if not isinstance(subject, URIRef):
                    continue
                subj = self._individual_from_uri(str(subject))

                if predicate == RDF.type and isinstance(obj, URIRef):
                    if obj == OWL.Ontology:
                        continue
                    cls = self._class_from_uri(str(obj))
                    self.base_ontology.add_axiom(OWLClassAssertionAxiom(subj, cls))
                    continue

                if not isinstance(predicate, URIRef):
                    continue
                pred_uri = str(predicate)

                if isinstance(obj, URIRef):
                    prop = self._object_property_from_uri(pred_uri)
                    obj_ind = self._individual_from_uri(str(obj))
                    self.base_ontology.add_axiom(OWLObjectPropertyAssertionAxiom(subj, prop, obj_ind))
                elif isinstance(obj, Literal):
                    prop = self._data_property_from_uri(pred_uri)
                    lit = self._owl_literal_from_rdflib_literal(obj)
                    self.base_ontology.add_axiom(OWLDataPropertyAssertionAxiom(subj, prop, lit))
        finally:
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass

    def _materialize_inferred_abox_ontology(self, asserted_ontology: Ontology, reasoner: SyncReasoner) -> Ontology:
        """Build an ontology containing asserted + inferred ABox assertions."""
        inferred_abox = SyncOntology(IRI.create("file:/final_inferred.owl"), load=False)

        # Keep all asserted ABox assertions.
        for ax in asserted_ontology.get_abox_axioms():
            inferred_abox.add_axiom(ax)

        skip_class_iris = {
            "http://www.w3.org/2002/07/owl#Thing",
            "http://www.w3.org/2002/07/owl#Nothing",
        }
        skip_object_property_iris = {
            "http://www.w3.org/2002/07/owl#topObjectProperty",
            "http://www.w3.org/2002/07/owl#bottomObjectProperty",
        }
        skip_data_property_iris = {
            "http://www.w3.org/2002/07/owl#topDataProperty",
            "http://www.w3.org/2002/07/owl#bottomDataProperty",
        }

        individuals = list(asserted_ontology.individuals_in_signature())
        classes = [
            cls
            for cls in asserted_ontology.classes_in_signature()
            if self._entity_str(cls) not in skip_class_iris
        ]
        object_properties = [
            prop
            for prop in asserted_ontology.object_properties_in_signature()
            if self._entity_str(prop) not in skip_object_property_iris
        ]
        data_properties = [
            prop
            for prop in asserted_ontology.data_properties_in_signature()
            if self._entity_str(prop) not in skip_data_property_iris
        ]

        object_property_values_supported = True
        data_property_values_supported = True

        for ind in individuals:
            for cls in reasoner.types(ind, direct=False):
                if not isinstance(cls, OWLClass):
                    continue
                if self._entity_str(cls) in skip_class_iris:
                    continue
                inferred_abox.add_axiom(OWLClassAssertionAxiom(ind, cls))

            if object_property_values_supported:
                for prop in object_properties:
                    try:
                        for obj in reasoner.object_property_values(ind, prop):
                            if isinstance(obj, OWLNamedIndividual):
                                inferred_abox.add_axiom(OWLObjectPropertyAssertionAxiom(ind, prop, obj))
                    except NotImplementedError:
                        object_property_values_supported = False
                        break

            if data_property_values_supported:
                for prop in data_properties:
                    try:
                        for obj in reasoner.data_property_values(ind, prop):
                            inferred_abox.add_axiom(OWLDataPropertyAssertionAxiom(ind, prop, obj))
                    except NotImplementedError:
                        data_property_values_supported = False
                        break

        return inferred_abox

    def _postprocess_inverse_object_properties(self, ontology: Ontology) -> Tuple[Ontology, int]:
        if not self._inverse_object_property_pairs:
            return ontology, 0

        graph = rdflib.Graph()
        for axiom in ontology.get_abox_axioms():
            if not isinstance(axiom, OWLObjectPropertyAssertionAxiom):
                continue
            graph.add(
                (
                    URIRef(self._entity_str(axiom.get_subject())),
                    URIRef(self._entity_str(axiom.get_property())),
                    URIRef(self._entity_str(axiom.get_object())),
                )
            )
        added_axioms: List[Tuple[str, str, str]] = []
        for forward_prop, inverse_prop in self._inverse_object_property_pairs:
            forward_ref = URIRef(forward_prop)
            inverse_ref = URIRef(inverse_prop)
            for subject, _, obj in list(graph.triples((None, forward_ref, None))):
                if not isinstance(subject, URIRef) or not isinstance(obj, URIRef):
                    continue
                inverse_triple = (obj, inverse_ref, subject)
                if inverse_triple in graph:
                    continue
                graph.add(inverse_triple)
                added_axioms.append((str(obj), inverse_prop, str(subject)))

        if not added_axioms:
            return ontology, 0

        for subject_uri, prop_uri, object_uri in added_axioms:
            ontology.add_axiom(
                OWLObjectPropertyAssertionAxiom(
                    self._individual_from_uri(subject_uri),
                    self._object_property_from_uri(prop_uri),
                    self._individual_from_uri(object_uri),
                )
            )

        return ontology, len(added_axioms)

    def _serialize_ontology(self, ontology: Ontology) -> str:
        with NamedTemporaryFile("w", suffix=".ttl", encoding="utf-8", delete=False) as tmp:
            out_path = Path(tmp.name)
        try:
            try:
                ontology.save(path=str(out_path), rdf_format="turtle")
            except TypeError:
                ontology.save(str(out_path))
            text = out_path.read_text(encoding="utf-8")
            return self.ensure_prefixes(text)
        finally:
            try:
                out_path.unlink(missing_ok=True)
            except Exception:
                pass

    def _remove_axiom_if_supported(self, ontology: Ontology, axiom: Any) -> bool:
        if hasattr(ontology, "remove_axiom"):
            ontology.remove_axiom(axiom)
            return True
        return False

    def _get_local_name(self, uri: str) -> str:
        if "#" in uri:
            return uri.rsplit("#", 1)[-1]
        return uri.rsplit("/", 1)[-1]

    def _tokenize(self, value: Optional[str]) -> List[str]:
        if not value:
            return []
        return [tok for tok in re.split(r"[^a-z0-9]+", value.lower()) if tok]

    def _coerce_optional_bool(self, value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if value is None:
            return None
        text = str(value).strip().lower()
        if text in {"true", "1", "yes"}:
            return True
        if text in {"false", "0", "no"}:
            return False
        return None

    def _get_node_data_assertions(self, ontology: Ontology, node: OWLNamedIndividual) -> Dict[str, List[Tuple[Any, Any]]]:
        by_prop: Dict[str, List[Tuple[Any, Any]]] = defaultdict(list)
        for ax in ontology.get_abox_axioms():
            if not isinstance(ax, OWLDataPropertyAssertionAxiom):
                continue
            if ax.get_subject() != node:
                continue
            by_prop[self._entity_str(ax.get_property())].append((ax, ax.get_object()))
        return by_prop

    def _first_data_object(self, values: List[Tuple[Any, Any]]) -> Optional[Any]:
        if not values:
            return None
        return values[0][1]

    def _first_bool_data_object(self, values: List[Tuple[Any, Any]]) -> Optional[bool]:
        for _, obj in values:
            parsed = self._coerce_optional_bool(self._entity_str(obj))
            if parsed is not None:
                return parsed
        return None

    def _index_abox(self, ontology: Ontology) -> Dict[str, Any]:
        by_class = defaultdict(set)
        object_assertions = defaultdict(lambda: defaultdict(set))
        data_assertions = defaultdict(lambda: defaultdict(set))
        inverse_targets = defaultdict(lambda: defaultdict(set))

        for ax in ontology.get_abox_axioms():
            if isinstance(ax, OWLClassAssertionAxiom):
                cls = ax.get_class_expression()
                if isinstance(cls, OWLClass):
                    by_class[self._entity_str(cls)].add(ax.get_individual())
            elif isinstance(ax, OWLObjectPropertyAssertionAxiom):
                s = ax.get_subject()
                p = ax.get_property()
                o = ax.get_object()
                s_key = self._entity_str(s)
                p_key = self._entity_str(p)
                object_assertions[s_key][p_key].add(o)
                inverse_targets[self._entity_str(o)][p_key].add(s)
            elif isinstance(ax, OWLDataPropertyAssertionAxiom):
                s = ax.get_subject()
                p = ax.get_property()
                o = ax.get_object()
                s_key = self._entity_str(s)
                p_key = self._entity_str(p)
                data_assertions[s_key][p_key].add(self._entity_str(o))

        return {
            "by_class": by_class,
            "object_assertions": object_assertions,
            "data_assertions": data_assertions,
            "inverse_targets": inverse_targets,
        }

    def _collect_value_nodes(self, abox_idx: Dict[str, Any]) -> List[OWLNamedIndividual]:
        iof_nodes = list(abox_idx["by_class"].get(self._entity_str(self.value_expression_class), set()))
        qudt_nodes = list(abox_idx["by_class"].get(self._entity_str(self.quantity_value_class), set()))
        nodes = {n for n in iof_nodes + qudt_nodes if isinstance(n, OWLNamedIndividual)}
        return sorted(nodes, key=lambda n: self._entity_str(n))

    def _assign_rows_to_nodes(
        self,
        value_nodes: List[OWLNamedIndividual],
        rows: List[Dict[str, Any]],
        abox_idx: Dict[str, Any],
    ) -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
        actions: List[Dict[str, Any]] = []
        assignments: Dict[str, Dict[str, Any]] = {}
        remaining_rows = rows[:]
        remaining_nodes = value_nodes[:]

        row_by_constraint = defaultdict(list)
        for row in remaining_rows:
            row_by_constraint[row.get("constraint_idx")].append(row)

        # Pass 1: parse explicit numeric suffix from local names, e.g. Value_3
        for node in value_nodes:
            node_uri = self._entity_str(node)
            local_name = self._get_local_name(node_uri)
            match = re.search(r"(?:^|_)(\d+)$", local_name)
            if not match:
                continue
            c_idx = int(match.group(1))
            candidates = [r for r in row_by_constraint.get(c_idx, []) if r in remaining_rows]
            if len(candidates) == 1:
                chosen = candidates[0]
                assignments[node_uri] = chosen
                remaining_rows.remove(chosen)
                remaining_nodes.remove(node)
                actions.append(
                    {
                        "node": node_uri,
                        "mapping": "constraint_idx_suffix",
                        "constraint_idx": chosen.get("constraint_idx"),
                    }
                )

        # Pass 2: lexical match using node label/name + linked bearer names vs attribute name
        for node in list(remaining_nodes):
            node_uri = self._entity_str(node)
            node_tokens = set(self._tokenize(self._get_local_name(node_uri)))
            inverse_links = abox_idx["inverse_targets"].get(node_uri, {})
            for prop in (self._entity_str(self.has_value_expr_some_time), self._entity_str(self.has_value_expr_all_times)):
                for bearer in inverse_links.get(prop, set()):
                    node_tokens.update(self._tokenize(self._get_local_name(self._entity_str(bearer))))

            scored = []
            for row in remaining_rows:
                attr_tokens = set(self._tokenize(row.get("attribute_name")))
                if not attr_tokens:
                    continue
                overlap = len(attr_tokens.intersection(node_tokens))
                if overlap > 0:
                    scored.append((overlap, row))
            scored.sort(key=lambda x: x[0], reverse=True)
            if scored and (len(scored) == 1 or scored[0][0] > scored[1][0]):
                chosen = scored[0][1]
                assignments[node_uri] = chosen
                remaining_rows.remove(chosen)
                remaining_nodes.remove(node)
                actions.append(
                    {
                        "node": node_uri,
                        "mapping": "lexical_attribute_match",
                        "constraint_idx": chosen.get("constraint_idx"),
                    }
                )

        # Pass 3: positional fallback only when cardinalities align
        if len(remaining_rows) == len(remaining_nodes):
            for node, row in zip(sorted(remaining_nodes, key=lambda n: self._entity_str(n)), remaining_rows):
                node_uri = self._entity_str(node)
                assignments[node_uri] = row
                actions.append(
                    {
                        "node": node_uri,
                        "mapping": "positional_fallback",
                        "constraint_idx": row.get("constraint_idx"),
                    }
                )
            remaining_rows = []
            remaining_nodes = []

        if remaining_rows or remaining_nodes:
            actions.append(
                {
                    "unresolved_rows": [
                        {
                            "constraint_idx": r.get("constraint_idx"),
                            "attribute_name": r.get("attribute_name"),
                            "unit_uri": r.get("unit_uri"),
                            "quantity_kind_uri": r.get("quantity_kind_uri"),
                        }
                        for r in remaining_rows
                    ],
                    "unresolved_value_nodes": [self._entity_str(n) for n in remaining_nodes],
                    "note": "Rule-based mapping is ambiguous for these entries.",
                }
            )

        return assignments, actions

    def llm_complete_qudt_enrichment(
        self,
        owl_text: str,
        unresolved_rows: List[Dict[str, Any]],
        unresolved_nodes: List[str],
        model: Optional[str] = None,
    ) -> str:
        selected_model = model or self.model
        system_prompt = (
            "You enrich OWL ABox graphs with IOF+QUDT links. "
            "Only add triples needed for ValueExpression/QuantityValue interpretation."
        )
        prompt = f"""
Apply IOF+QUDT modeling to unresolved quantitative mappings.

Rules:
- Keep existing individuals and IRIs; do not delete triples.
- For each quantitative value expression, ensure:
  - rdf:type iof:ValueExpression
  - rdf:type qudt:QuantityValue
  - qudt:unit (URI individual), typed qudt:Unit when unit is known
  - connect unit -> qudt:hasQuantityKind -> quantity kind when known
  - qudt:numericValue, or bound properties if range constraints are present
  - when inclusion booleans are explicit, use min/max inclusive/exclusive properties; otherwise use qudt:lowerBound/qudt:upperBound
  - NEVER use qudt:lowerBoundInclusive or qudt:upperBoundInclusive.
  - if qudt:numericValue or any QUDT bounds are present on a node, do not keep iof:hasSimpleExpressionValue on that node
  - inverse iof:isValueExpressionOfAtSomeTime / iof:isValueExpressionOfAtAllTimes when corresponding forward links exist
- Prefer iof:hasValueExpressionAtSomeTime / iof:hasValueExpressionAtAllTimes over measured-value properties.
        - Return OWL/Turtle only.
        - Never return RDF/XML or XML declarations.

Unresolved normalization rows:
{json.dumps(unresolved_rows, indent=2)}

Unresolved value nodes:
{json.dumps(unresolved_nodes, indent=2)}

Current graph:
{owl_text}
"""
        completion = self._chat_completion_create(
            model=selected_model,
            temperature=0,
            cache_scope="qudt-unresolved",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
        )
        return self._coerce_to_turtle_text(completion.choices[0].message.content)

    def _validate_iof_qudt_pattern(self, owl_text: str, payload: Optional[Dict[str, Any]] = None) -> List[str]:
        g = self._parse_graph_from_text(owl_text)

        iof_value_expr = URIRef(f"{self.iof_ns}ValueExpression")
        qudt_quantity_value = URIRef(f"{self.qudt_ns}QuantityValue")
        qudt_unit_cls = URIRef(f"{self.qudt_ns}Unit")

        p_unit = URIRef(f"{self.qudt_ns}unit")
        p_has_qk = URIRef(f"{self.qudt_ns}hasQuantityKind")
        p_numeric = URIRef(f"{self.qudt_ns}numericValue")
        p_lower = URIRef(f"{self.qudt_ns}lowerBound")
        p_upper = URIRef(f"{self.qudt_ns}upperBound")
        p_min_inc = URIRef(f"{self.qudt_ns}minInclusive")
        p_max_inc = URIRef(f"{self.qudt_ns}maxInclusive")
        p_min_exc = URIRef(f"{self.qudt_ns}minExclusive")
        p_max_exc = URIRef(f"{self.qudt_ns}maxExclusive")
        p_low_inc_bad = URIRef(f"{self.qudt_ns}lowerBoundInclusive")
        p_up_inc_bad = URIRef(f"{self.qudt_ns}upperBoundInclusive")
        qk_type = URIRef(f"{self.qudt_ns}QuantityKind")
        p_simple = URIRef(f"{self.iof_ns}hasSimpleExpressionValue")

        measured_props = [
            URIRef(f"{self.iof_ns}hasMeasuredValueAtSomeTime"),
            URIRef(f"{self.iof_ns}hasMeasuredValueAtAllTimes"),
            URIRef(f"{self.iof_ns}isMeasuredValueOfAtSomeTime"),
            URIRef(f"{self.iof_ns}isMeasuredValueOfAtAllTimes"),
        ]

        violations: List[str] = []
        for bad_p in (p_low_inc_bad, p_up_inc_bad):
            if any(g.triples((None, bad_p, None))):
                violations.append(f"Found forbidden predicate: {bad_p}")
        for mp in measured_props:
            if any(g.triples((None, mp, None))):
                violations.append(f"Found measured-value predicate that must be replaced: {mp}")

        value_nodes = set(g.subjects(RDF.type, iof_value_expr)) | set(g.subjects(RDF.type, qudt_quantity_value))
        expected_bounds: Dict[Tuple[int, int], Dict[str, Any]] = {}
        for row in self.flatten_normalization(payload or self.payload):
            req_idx = row.get("req_idx")
            c_idx = row.get("constraint_idx")
            if isinstance(req_idx, int) and isinstance(c_idx, int):
                expected_bounds[(req_idx, c_idx)] = row

        bound_preds = [p_lower, p_upper, p_min_inc, p_max_inc, p_min_exc, p_max_exc]
        for node in sorted(value_nodes, key=lambda x: str(x)):
            if any(g.triples((node, p_has_qk, None))):
                violations.append(f"ValueExpression node has qudt:hasQuantityKind (must be on unit): {node}")

            units = list(g.objects(node, p_unit))
            if not units:
                violations.append(f"ValueExpression/QuantityValue missing qudt:unit: {node}")
                continue

            for unit in units:
                if (unit, RDF.type, qudt_unit_cls) not in g:
                    violations.append(f"Unit node missing rdf:type qudt:Unit: {unit}")
                unit_qks = list(g.objects(unit, p_has_qk))
                if not unit_qks:
                    violations.append(f"Unit missing qudt:hasQuantityKind: {unit}")
                for qk in unit_qks:
                    if (qk, RDF.type, qk_type) not in g:
                        violations.append(f"QuantityKind node missing rdf:type qudt:QuantityKind: {qk}")

            has_numeric = any(g.triples((node, p_numeric, None)))
            has_bounds = any(any(g.triples((node, bp, None))) for bp in bound_preds)
            if has_numeric and has_bounds:
                violations.append(f"Node has both qudt:numericValue and bounds; keep only bounds: {node}")
            has_simple = any(g.triples((node, p_simple, None)))
            if (has_numeric or has_bounds) and has_simple:
                violations.append(
                    f"Node has QUDT numeric/bound values and iof:hasSimpleExpressionValue; keep only QUDT data properties: {node}"
                )

            node_local = str(node).rsplit("#", 1)[-1]
            m = re.match(r"VE_req(\d+)_c(\d+)$", node_local)
            if m:
                key = (int(m.group(1)), int(m.group(2)))
                expected = expected_bounds.get(key)
                if expected:
                    needs_lower = expected.get("lower_bound") is not None
                    needs_upper = expected.get("upper_bound") is not None
                    has_lower = any(any(g.triples((node, p, None))) for p in (p_lower, p_min_inc, p_min_exc))
                    has_upper = any(any(g.triples((node, p, None))) for p in (p_upper, p_max_inc, p_max_exc))
                    if needs_lower and not has_lower:
                        violations.append(f"Expected lower bound missing for node: {node}")
                    if needs_upper and not has_upper:
                        violations.append(f"Expected upper bound missing for node: {node}")

        return sorted(set(violations))

    def _sanitize_specification_has_specified_output_usage(
        self,
        owl_text: str,
    ) -> Tuple[str, List[Dict[str, Any]]]:
        graph = self._parse_graph_from_text(owl_text)
        has_specified_output = URIRef(f"{self.iof_ns}hasSpecifiedOutput")
        prescribes = URIRef(f"{self.iof_ns}prescribes")
        actions: List[Dict[str, Any]] = []

        for subject, _, obj in sorted(graph.triples((None, has_specified_output, None)), key=lambda t: (str(t[0]), str(t[2]))):
            if not isinstance(subject, URIRef):
                continue

            subject_types = [t for t in graph.objects(subject, RDF.type) if isinstance(t, URIRef)]
            local_type_names = {self._get_local_name(str(t)) for t in subject_types}
            is_specification_like = (
                any(name.endswith("Specification") for name in local_type_names)
                or "InformationContentEntity" in local_type_names
            )
            if not is_specification_like:
                continue

            graph.remove((subject, has_specified_output, obj))
            if isinstance(obj, URIRef):
                graph.add((subject, prescribes, obj))
            actions.append(
                {
                    "node": str(subject),
                    "fix": "replaced iof:hasSpecifiedOutput on specification-like subject with iof:prescribes",
                    "target": str(obj),
                }
            )

        if not actions:
            return owl_text, []

        ttl = graph.serialize(format="turtle")
        if isinstance(ttl, bytes):
            ttl = ttl.decode("utf-8")
        return self.ensure_prefixes(self._normalize_qudt_prefix_aliases(ttl)), actions

    def _sanitize_qudt_valueexpression_literals(
        self,
        owl_text: str,
    ) -> Tuple[str, List[Dict[str, Any]]]:
        graph = self._parse_graph_from_text(owl_text)
        iof_value_expr = URIRef(f"{self.iof_ns}ValueExpression")
        qudt_quantity_value = URIRef(f"{self.qudt_ns}QuantityValue")
        p_simple = URIRef(f"{self.iof_ns}hasSimpleExpressionValue")
        qudt_data_predicates = [
            URIRef(f"{self.qudt_ns}numericValue"),
            URIRef(f"{self.qudt_ns}lowerBound"),
            URIRef(f"{self.qudt_ns}upperBound"),
            URIRef(f"{self.qudt_ns}minInclusive"),
            URIRef(f"{self.qudt_ns}maxInclusive"),
            URIRef(f"{self.qudt_ns}minExclusive"),
            URIRef(f"{self.qudt_ns}maxExclusive"),
            URIRef(f"{self.qudt_ns}lowerBoundInclusive"),
            URIRef(f"{self.qudt_ns}upperBoundInclusive"),
        ]

        value_nodes = set(graph.subjects(RDF.type, iof_value_expr)) | set(graph.subjects(RDF.type, qudt_quantity_value))
        actions: List[Dict[str, Any]] = []

        for node in sorted(value_nodes, key=lambda n: str(n)):
            has_qudt_data = any(any(graph.triples((node, pred, None))) for pred in qudt_data_predicates)
            if not has_qudt_data:
                continue
            simple_values = list(graph.objects(node, p_simple))
            if not simple_values:
                continue
            for literal in simple_values:
                graph.remove((node, p_simple, literal))
            actions.append(
                {
                    "node": str(node),
                    "fix": "removed iof:hasSimpleExpressionValue because QUDT numeric/bound properties are present",
                    "removed_count": len(simple_values),
                }
            )

        if not actions:
            return owl_text, []

        ttl = graph.serialize(format="turtle")
        if isinstance(ttl, bytes):
            ttl = ttl.decode("utf-8")
        return self.ensure_prefixes(self._normalize_qudt_prefix_aliases(ttl)), actions

    def _is_likely_occurrent_target(self, graph: rdflib.Graph, node: URIRef) -> bool:
        bfo_ns = "http://purl.obolibrary.org/obo/"
        occurrent_markers = {
            URIRef(f"{bfo_ns}BFO_0000003"),  # Occurrent
            URIRef(f"{bfo_ns}BFO_0000015"),  # Process
            URIRef(f"{self.iof_ns}PlannedProcess"),
            URIRef(f"{self.iof_ns}ProcessCharacteristic"),
            URIRef(f"{self.iof_ns}Event"),
        }
        node_types = set(graph.objects(node, RDF.type))
        if any(node_type in occurrent_markers for node_type in node_types):
            return True

        for node_type in node_types:
            local_name = self._get_local_name(str(node_type)).lower()
            if "process" in local_name or "event" in local_name or "occurrent" in local_name:
                return True
        return False

    def _sanitize_design_specification_usage(
        self,
        owl_text: str,
    ) -> Tuple[str, List[Dict[str, Any]]]:
        graph = self._parse_graph_from_text(owl_text)
        design_spec = URIRef(f"{self.iof_ns}DesignSpecification")
        plan_spec = URIRef(f"{self.iof_ns}PlanSpecification")
        prescribes = URIRef(f"{self.iof_ns}prescribes")
        actions: List[Dict[str, Any]] = []

        for spec in sorted(set(graph.subjects(RDF.type, design_spec)), key=lambda x: str(x)):
            bad_targets: List[str] = []
            for target in graph.objects(spec, prescribes):
                if isinstance(target, URIRef) and self._is_likely_occurrent_target(graph, target):
                    bad_targets.append(str(target))

            if not bad_targets:
                continue

            graph.remove((spec, RDF.type, design_spec))
            graph.add((spec, RDF.type, plan_spec))
            actions.append(
                {
                    "node": str(spec),
                    "fix": "retyped DesignSpecification to PlanSpecification because it prescribes occurrent targets",
                    "targets": bad_targets,
                }
            )

        if not actions:
            return owl_text, []

        ttl = graph.serialize(format="turtle")
        if isinstance(ttl, bytes):
            ttl = ttl.decode("utf-8")
        return self.ensure_prefixes(self._normalize_qudt_prefix_aliases(ttl)), actions

    def _normalize_qudt_prefix_aliases(self, ttl_text: str) -> str:
        text = ttl_text
        text = re.sub(
            r"@prefix\s+unit:\s*<http://qudt\.org/vocab/unit/>\s*\.",
            "@prefix qudtu: <http://qudt.org/vocab/unit/> .",
            text,
        )
        text = re.sub(
            r"@prefix\s+quantitykind:\s*<http://qudt\.org/vocab/quantitykind/>\s*\.",
            "@prefix qudtqk: <http://qudt.org/vocab/quantitykind/> .",
            text,
        )
        text = re.sub(r"\bunit:", "qudtu:", text)
        text = re.sub(r"\bquantitykind:", "qudtqk:", text)
        return text

    def llm_repair_qudt_graph(
        self,
        owl_text: str,
        payload: Dict[str, Any],
        violations: List[str],
        model: Optional[str] = None,
    ) -> str:
        selected_model = model or self.model
        normalized_brief = self.build_normalized_record_brief(payload)
        quant_row_count = len(self.flatten_normalization(payload))
        system_prompt = (
            "You repair OWL graphs to satisfy IOF+QUDT pattern constraints. "
            "Do not remove non-quantitative requirement semantics."
        )
        prompt = f"""
Repair the graph so that it follows this IOF+QUDT pattern exactly:
- Quantitative value node: rdf:type iof:ValueExpression and qudt:QuantityValue.
- Value node uses qudt:unit -> Unit node.
- Quantity kind must be on Unit: Unit qudt:hasQuantityKind QuantityKind.
- Each quantity kind resource must be explicitly typed: `a qudt:QuantityKind`.
- Do NOT place qudt:hasQuantityKind on ValueExpression nodes.
- Bounds use qudt:minInclusive/maxInclusive or qudt:minExclusive/maxExclusive, or qudt:lowerBound/qudt:upperBound when inclusivity is unknown.
- Never use qudt:lowerBoundInclusive or qudt:upperBoundInclusive.
- If bounds exist, remove qudt:numericValue.
- If qudt:numericValue or any QUDT bounds are present on a node, remove iof:hasSimpleExpressionValue from that node.
- Use iof:hasValueExpressionAtSomeTime/AtAllTimes and inverses; do not use measured-value predicates.
- Keep existing IRIs and requirement meaning.
- Prefer `qudtu:` for unit individuals and `qudtqk:` for quantity-kind individuals in Turtle serialization.
- Guardrail: only quantitative constraints from NormalizedRecord may use iof:ValueExpression / qudt:QuantityValue.
- If NormalizedRecord has zero quantitative rows, remove any accidental iof:ValueExpression/qudt:* quantitative nodes and return a graph with no quantitative modeling.

NormalizedRecord quantitative row count:
{quant_row_count}

NormalizedRecord quantitative summary:
{normalized_brief}

Detected violations:
{json.dumps(violations, indent=2)}

Graph to repair:
{owl_text}

Return OWL/Turtle only.
Never return RDF/XML or XML declarations.
"""
        completion = self._chat_completion_create(
            model=selected_model,
            temperature=0,
            cache_scope="qudt-repair",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
        )
        return self._coerce_to_turtle_text(completion.choices[0].message.content)

    def llm_apply_qudt_guideline(
        self,
        owl_text: str,
        payload: Dict[str, Any],
        model: Optional[str] = None,
        max_iters: int = 3,
    ) -> Tuple[str, List[Dict[str, Any]]]:
        selected_model = model or self.model
        normalized_brief = self.build_normalized_record_brief(payload)
        quant_row_count = len(self.flatten_normalization(payload))
        system_prompt = (
            "You are an ontological engineer specialized in IOF and QUDT integration. "
            "Output OWL/Turtle only."
        )
        user_prompt = f"""
Enrich this IOF graph with QUDT according to the IOF-QUDT guideline.

Mandatory pattern:
- Quantitative value nodes must be iof:ValueExpression and qudt:QuantityValue.
- Connect value node to unit with qudt:unit.
- Connect unit node to quantity kind with qudt:hasQuantityKind.
- Quantity kind nodes must be explicitly declared as individuals with `a qudt:QuantityKind`.
- Prefer `qudtu:` for unit individuals and `qudtqk:` for quantity-kind individuals in Turtle serialization.
- Do not use qudt:hasQuantityKind on value nodes.
- Do not use iof:*MeasuredValue* properties.
- For bounded constraints, use:
  - qudt:minInclusive / qudt:maxInclusive when inclusive
  - qudt:minExclusive / qudt:maxExclusive when exclusive
  - qudt:lowerBound / qudt:upperBound when inclusivity is unknown
- Never use qudt:lowerBoundInclusive or qudt:upperBoundInclusive.
- If bounds are present, do not keep qudt:numericValue.
- If qudt:numericValue or any QUDT bounds are present on a node, do not keep iof:hasSimpleExpressionValue on that node.
- Guardrail: apply QUDT ONLY to constraints that are quantitative in NormalizedRecord.
- If NormalizedRecord has zero quantitative rows, return the graph unchanged except for removing accidental quantitative artifacts (`iof:ValueExpression`, `qudt:*` nodes/triples) that are not backed by NormalizedRecord.
- Never create `iof:ValueExpression` or `qudt:QuantityValue` from textual conformance/reference constraints.

NormalizedRecord quantitative row count:
{quant_row_count}

NormalizedRecord quantitative summary:
{normalized_brief}

Graph:
{owl_text}
"""
        completion = self._chat_completion_create(
            model=selected_model,
            temperature=0,
            cache_scope="qudt-guideline",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        enriched = self.ensure_prefixes(self._coerce_to_turtle_text(completion.choices[0].message.content))
        enriched = self._normalize_qudt_prefix_aliases(enriched)
        actions: List[Dict[str, Any]] = [{"note": "Applied LLM QUDT enrichment stage."}]

        for i in range(max_iters):
            violations = self._validate_iof_qudt_pattern(enriched, payload=payload)
            if not violations:
                actions.append({"note": f"QUDT validation passed after {i} repair iterations."})
                return enriched, actions
            actions.append({"iter": i, "violations": violations})
            enriched = self.ensure_prefixes(
                self.llm_repair_qudt_graph(enriched, payload, violations, model=selected_model)
            )
            enriched = self._normalize_qudt_prefix_aliases(enriched)

        actions.append({"note": "QUDT validation still has violations after max repair iterations."})
        return enriched, actions

    def _safe_read(self, path: Path) -> str:
        try:
            return path.read_text(encoding="utf-8", errors="ignore")
        except FileNotFoundError:
            print(f"Missing context file: {path}")
            return ""

    def _load_tbox_axioms_from_source(self, source_path: Path) -> List[Any]:
        temp_path: Optional[Path] = None
        load_path = source_path
        try:
            if source_path.suffix.lower() in {".rdf", ".owl", ".xml"}:
                text = source_path.read_text(encoding="utf-8", errors="ignore")
                stripped = re.sub(r"<owl:imports\b[^>]*/>", "", text, flags=re.IGNORECASE)
                stripped = re.sub(
                    r"<owl:imports\b[^>]*>.*?</owl:imports>",
                    "",
                    stripped,
                    flags=re.IGNORECASE | re.DOTALL,
                )
                if stripped != text:
                    with NamedTemporaryFile("w", suffix=source_path.suffix, encoding="utf-8", delete=False) as tmp:
                        tmp.write(stripped)
                        temp_path = Path(tmp.name)
                    load_path = temp_path
            return list(SyncOntology(str(load_path)).get_tbox_axioms())
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except Exception:
                    pass

    def _load_tbox_axioms(self) -> List[Any]:
        """Load local TBox axioms without remote import resolution."""
        source_path = self.tbox_path if self.tbox_path.exists() else self.CORE_PATH
        axiom_sources: List[Path] = [source_path]
        annotation_vocab = self.ONTOLOGY_DIR / "AnnotationVocabulary.rdf"
        bfo_core = self.ONTOLOGY_DIR / "bfo-core.owl"
        if annotation_vocab.exists():
            axiom_sources.append(annotation_vocab)
        if bfo_core.exists():
            axiom_sources.append(bfo_core)

        axioms: List[Any] = []
        for path in axiom_sources:
            axioms.extend(self._load_tbox_axioms_from_source(path))
        return axioms

    def _load_inverse_object_property_pairs(self) -> List[Tuple[str, str]]:
        source_path = self.tbox_path if self.tbox_path.exists() else self.CORE_PATH
        ontology_sources: List[Path] = [source_path]
        annotation_vocab = self.ONTOLOGY_DIR / "AnnotationVocabulary.rdf"
        bfo_core = self.ONTOLOGY_DIR / "bfo-core.owl"
        if annotation_vocab.exists():
            ontology_sources.append(annotation_vocab)
        if bfo_core.exists():
            ontology_sources.append(bfo_core)

        inverse_pairs: set[Tuple[str, str]] = set()
        for path in ontology_sources:
            graph = rdflib.Graph()
            try:
                graph.parse(path)
            except Exception:
                continue
            for prop, _, inverse in graph.triples((None, OWL.inverseOf, None)):
                if not isinstance(prop, URIRef) or not isinstance(inverse, URIRef):
                    continue
                inverse_pairs.add((str(prop), str(inverse)))
                inverse_pairs.add((str(inverse), str(prop)))
        return sorted(inverse_pairs)

    def agentic_loop(self) -> Dict[str, Any]:
        owl = self.llm_build_graph(self.client, self.payload)
        owl = self.ensure_prefixes(owl)
        base = f"http://example.org/req/{self.payload.get('idx','demo')}"
        owl = self.enforce_base(owl, base)
        owl = self.add_ontology_header(owl, base if base.endswith(('#','/')) else base + '#')
        history = []
        for step in range(self.max_iters):
            owl, spec_output_actions = self._sanitize_specification_has_specified_output_usage(owl)
            if spec_output_actions:
                print(f"Applied {len(spec_output_actions)} iof:hasSpecifiedOutput-to-prescribes repair(s) before reasoning.")
            owl, structural_actions = self._sanitize_design_specification_usage(owl)
            if structural_actions:
                print(f"Applied {len(structural_actions)} structural IOF repair(s) before reasoning.")
            owl, value_literal_actions = self._sanitize_qudt_valueexpression_literals(owl)
            if value_literal_actions:
                print(f"Applied {len(value_literal_actions)} QUDT/simple-expression cleanup repair(s) before reasoning.")
            self._update_base_ontology_from_owl(owl)
            success, msg, onto, reasoner = self.reason()
            print(msg)
            if success:
                self.inferred_ontology = onto

                history.append({
                    "iter": step,
                    "owl": owl,
                    "pellet_ok": success,
                    "pellet_report": msg
                })
                print(f"Graph valid after {step} iterations.")
                return {"owl": owl, "history": history}

            else:
                owl = self.llm_repair_graph(owl, msg)

        return {"owl": owl, "history": history}

    def raw_agentic_loop(self) -> Dict[str, Any]:
        owl = self.llm_build_graph_from_raw_requirement(self.client, self.payload)
        owl = self.ensure_prefixes(owl)
        base = f"http://example.org/req/{self.payload.get('idx','demo')}"
        owl = self.enforce_base(owl, base)
        owl = self.add_ontology_header(owl, base if base.endswith(('#','/')) else base + '#')
        initial_owl = owl
        history = []
        for step in range(self.max_iters):
            owl, spec_output_actions = self._sanitize_specification_has_specified_output_usage(owl)
            if spec_output_actions:
                print(f"Applied {len(spec_output_actions)} iof:hasSpecifiedOutput-to-prescribes repair(s) before reasoning.")
            owl, structural_actions = self._sanitize_design_specification_usage(owl)
            if structural_actions:
                print(f"Applied {len(structural_actions)} structural IOF repair(s) before reasoning.")
            owl, value_literal_actions = self._sanitize_qudt_valueexpression_literals(owl)
            if value_literal_actions:
                print(f"Applied {len(value_literal_actions)} QUDT/simple-expression cleanup repair(s) before reasoning.")
            self._update_base_ontology_from_owl(owl)
            success, msg, onto, reasoner = self.reason()
            print(msg)
            if success:
                self.inferred_ontology = onto
                history.append({
                    "iter": step,
                    "owl": owl,
                    "pellet_ok": success,
                    "pellet_report": msg
                })
                print(f"Graph valid after {step} iterations.")
                return {"initial_owl": initial_owl, "owl": owl, "history": history}
            owl = self.llm_repair_graph_from_raw_requirement(owl, msg)

        return {"initial_owl": initial_owl, "owl": owl, "history": history}

    def strip_code_fence(self, text: str) -> str:
        if text is None:
            return ""
        text = text.strip()
        if text.startswith("```"):
            parts = text.split("```", 2)
            text = parts[1].strip() if len(parts) > 1 else text
        # drop accidental leading language tags like 'turtle'/'rdfxml' on a single line
        lower = text.lower()
        if (
            lower.startswith("turtle")
            or lower.startswith("ttl")
            or lower.startswith("owl")
            or lower.startswith("rdf")
            or lower.startswith("xml")
            or lower.startswith("rdfxml")
        ):
            text = text.split("\n", 1)[1].lstrip() if "\n" in text else ""
        return text

    def _remove_inline_language_markers(self, text: str) -> str:
        # Some models emit a standalone `ttl`/`turtle`/`xml` line even after fence stripping.
        # Remove those marker-only lines before parsing.
        markers = {"ttl", "turtle", "rdf", "owl", "xml", "rdfxml"}
        cleaned_lines: List[str] = []
        for line in text.splitlines():
            if line.strip().lower() in markers:
                continue
            cleaned_lines.append(line)
        return "\n".join(cleaned_lines).strip()

    def _extract_xml_fragment(self, text: str) -> Optional[str]:
        markers = ("<?xml", "<rdf:rdf", "<rdf:RDF", "<owl:Ontology")
        candidates = [text.find(marker) for marker in markers if text.find(marker) != -1]
        if not candidates:
            return None
        start = min(candidates)
        return text[start:].strip()

    def _serialize_graph_turtle(self, graph: rdflib.Graph) -> str:
        ttl = graph.serialize(format="turtle")
        if isinstance(ttl, bytes):
            ttl = ttl.decode("utf-8")
        return ttl

    def _coerce_to_turtle_text(self, text: str) -> str:
        cleaned = self._remove_inline_language_markers(self.strip_code_fence(text))
        if not cleaned:
            return ""

        # Fast path: already valid Turtle.
        graph = rdflib.Graph()
        try:
            graph.parse(data=cleaned, format="turtle")
            return self._serialize_graph_turtle(graph)
        except Exception:
            pass

        # Fallback: if XML appears (possibly after stray Turtle prefixes), extract and convert.
        xml_fragment = self._extract_xml_fragment(cleaned)
        if xml_fragment:
            graph = rdflib.Graph()
            try:
                graph.parse(data=xml_fragment, format="xml")
                return self._serialize_graph_turtle(graph)
            except Exception:
                pass

        # Last attempt: parse full text as RDF/XML.
        graph = rdflib.Graph()
        try:
            graph.parse(data=cleaned, format="xml")
            return self._serialize_graph_turtle(graph)
        except Exception:
            return cleaned

    def _parse_graph_from_text(self, owl_text: str) -> rdflib.Graph:
        raw = self._remove_inline_language_markers(self.strip_code_fence(owl_text))
        text = self.ensure_prefixes(raw)
        graph = rdflib.Graph()
        try:
            graph.parse(data=text, format="turtle")
            return graph
        except Exception:
            pass

        xml_fragment = self._extract_xml_fragment(raw)
        if xml_fragment:
            graph = rdflib.Graph()
            graph.parse(data=xml_fragment, format="xml")
            return graph

        graph = rdflib.Graph()
        graph.parse(data=raw, format="xml")
        return graph

    def llm_build_graph(self, client: Optional[OpenAI] = None, payload: Optional[dict] = None) -> str:
        """Build OWL graph text via LLM using provided payload/client or fallbacks."""
        payload = payload or self.payload
        client = client or self.client
        normalized_brief = self.build_normalized_record_brief(payload)
        system_prompt = self.SYSTEM_PROMPT_TEMPLATE.format(
            record_idx=payload.get("idx", "demo"),
            ontology_context=self.CORE_CONTEXT,
        )
        user_prompt = (
            "NormalizedRecord quantitative summary (derived from normalized_quantities):\n"
            + normalized_brief
            + "\n\nStructured requirement payload:\n"
            + json.dumps(payload, indent=2)
        )
        completion = self._chat_completion_create(
            client=client,
            model=self.model,
            temperature=self.temperature,
            cache_scope="build-structured",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        return self._coerce_to_turtle_text(completion.choices[0].message.content)

    def llm_build_graph_zero_shot(self, client: Optional[OpenAI] = None, payload: Optional[dict] = None) -> str:
        """Build OWL graph text via a single LLM call with inline IOF+QUDT grounding guidance."""
        payload = payload or self.payload
        client = client or self.client
        normalized_brief = self.build_normalized_record_brief(payload)
        system_prompt = self.ZERO_SHOT_SYSTEM_PROMPT_TEMPLATE.format(
            record_idx=payload.get("idx", "demo"),
            ontology_context=self.CORE_CONTEXT,
            zero_shot_iof_qudt_rules=self.ZERO_SHOT_IOF_QUDT_RULES.strip(),
        )
        user_prompt = (
            "NormalizedRecord quantitative summary (derived from normalized_quantities):\n"
            + normalized_brief
            + "\n\nStructured requirement payload:\n"
            + json.dumps(payload, indent=2)
        )
        completion = self._chat_completion_create(
            client=client,
            model=self.model,
            temperature=self.temperature,
            cache_scope="build-zero-shot",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        return self._coerce_to_turtle_text(completion.choices[0].message.content)

    def llm_build_graph_from_raw_requirement(
        self,
        client: Optional[OpenAI] = None,
        payload: Optional[dict] = None,
    ) -> str:
        """Build OWL graph text directly from the raw requirement sentence."""
        payload = payload or self.payload
        client = client or self.client
        record_idx = payload.get("idx", "demo")
        original_text = str(payload.get("original_text") or "").strip()
        system_prompt = dedent(
            f"""
        You are an ontological engineer. Given a raw requirement sentence, produce an OWL graph in Turtle (.ttl) format.

        Inputs you must use directly:
        - Requirement sentence text (original_text field of the input record).
        - Full IOF Core ontology (Core.rdf, RDF/XML) as authoritative vocabulary and axioms.

        Goals (in order):
        1) Map the whole requirement into IOF classes and properties. Do not define new classes or properties of any kind.
        2) Avoid blank nodes; mint readable IRIs under the base.
        3) For true quantitative constraints, emit the IOF + QUDT pattern directly in this single pass.
        4) Reuse individuals rather than duplicating (e.g., the same quality/agent that the value qualifies).
        5) Keep the graph OWL DL compatible and minimal.

        Graph pattern constraints (mandatory):

        - Create exactly one iof:RequirementSpecification individual :Req_0 and attach a comment to it with the original_text of the input requirement.
        - The requirement specification individual must be connected to a specification instance via iof:requirementSatisfiedBy.
        - Use iof:DesignSpecification when the requirement constrains continuants (artifacts/material entities/qualities of continuants).
        - Use iof:PlanSpecification when the requirement constrains processes, events, procedures, or process characteristics.
        - iof:hasSpecifiedOutput has domain iof:PlannedProcess. Never use iof:hasSpecifiedOutput on any specification individual (DesignSpecification / PlanSpecification / RequirementSpecification / ObjectiveSpecification / InformationContentEntity). Use iof:prescribes for specification semantics.
        - A iof:DesignSpecification MUST NOT prescribe occurrents. In particular, never assert:
          - DesignSpecification iof:prescribes Process
          - DesignSpecification iof:prescribes PlannedProcess
          - DesignSpecification iof:prescribes ProcessCharacteristic
          - DesignSpecification iof:prescribes Event
        - If a specification prescribes any process-like entity, type it as iof:PlanSpecification (not iof:DesignSpecification).
        - This prompt receives one raw requirement sentence only, so use `req_idx = 0`.
        - If the sentence contains multiple distinct quantitative constraints, number them `constraint_idx = 0, 1, 2, ...` in textual order.
        {self.ZERO_SHOT_IOF_QUDT_RULES.strip()}
        - Infer the most appropriate QUDT unit and quantity kind directly from the sentence when the text is explicit enough to support them.

        Mandatory prefixes (always include):
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        @prefix iof: <https://spec.industrialontologies.org/ontology/core/Core/> .
        @prefix bfo: <http://purl.obolibrary.org/obo/> .
        @prefix qudt: <http://qudt.org/schema/qudt/> .
        @prefix qudtu: <http://qudt.org/vocab/unit/> .
        @prefix qudtqk: <http://qudt.org/vocab/quantitykind/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix : <http://example.org/req/{record_idx}#> .

        Authoritative ontology (full Core.rdf content):
        {self.CORE_CONTEXT}

        Return ONLY OWL/Turtle (TTL syntax), no commentary, no RDF/XML, base IRI http://example.org/req/{record_idx}#
        Before final output, verify that `:Req_0` exists and every quantitative constraint found in the sentence has one corresponding `:VE_req0_c<constraint_idx>` node.
            """
        )
        user_prompt = f"Raw requirement sentence:\n{original_text}"
        completion = self._chat_completion_create(
            client=client,
            model=self.model,
            temperature=self.temperature,
            cache_scope="build-raw",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        return self._coerce_to_turtle_text(completion.choices[0].message.content)

    def ensure_prefixes(self, ttl_text: str) -> str:
        ttl_text = self._coerce_to_turtle_text(ttl_text)
        if self._extract_xml_fragment(ttl_text):
            return ttl_text
        required = {
            'iof': 'https://spec.industrialontologies.org/ontology/core/Core/',
            'qudt': 'http://qudt.org/schema/qudt/',
            'qudtu': 'http://qudt.org/vocab/unit/',
            'qudtqk': 'http://qudt.org/vocab/quantitykind/',
            'bfo': 'http://purl.obolibrary.org/obo/',
            'xsd': 'http://www.w3.org/2001/XMLSchema#',
            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
            'owl': 'http://www.w3.org/2002/07/owl#'
        }
        lines = ttl_text.splitlines()
        present = set()
        for ln in lines[:20]:
            if ln.startswith('@prefix'):
                parts = ln.split()
                if len(parts) >= 3:
                    pref = parts[1].rstrip(':')
                    present.add(pref)
        missing = [p for p in required if p not in present]
        if missing:
            prefix_block = '\n'.join(['@prefix {}: <{}> .'.format(p, required[p]) for p in missing]) + '\n'
            ttl_text = prefix_block + ttl_text
        return ttl_text

    def enforce_base(self, ttl_text: str, base: str) -> str:
        base = base.strip()
        if not (base.endswith('#') or base.endswith('/')):
            base = base + '#'
        if '@base' in ttl_text or '@prefix :' in ttl_text:
            return ttl_text
        return f"@base <{base}> .\n{ttl_text}"

    def add_ontology_header(self, ttl_text: str, base: str) -> str:
        """Normalize ordering: base, prefixes, body, ontology declaration."""
        lines = [ln.strip() for ln in ttl_text.splitlines() if ln.strip()]
        base_line = f"@base <{base}> ."
        bases = [ln for ln in lines if ln.startswith("@base")]
        prefixes = [ln for ln in lines if ln.startswith("@prefix")]
        body = [ln for ln in lines if not (ln.startswith("@base") or ln.startswith("@prefix") or " a owl:Ontology" in ln)]

        # replace/ensure single base line
        lines_out = [base_line]
        # unique prefixes preserving order appearance
        seen = set()
        for ln in prefixes:
            if ln not in seen:
                lines_out.append(ln)
                seen.add(ln)

        lines_out.extend(body)

        ontology_decl = f"<{base}> a owl:Ontology ."
        if ontology_decl not in lines_out:
            lines_out.append(ontology_decl)

        return "\n".join(lines_out) + "\n"

    def reason(self) -> Tuple[bool, str, Any, Any]:
        # 1. Create an empty SyncOntology  
        combined = SyncOntology(IRI.create("file:/combined.owl"), load=False)  
        # 2. Add TBox axioms from a TBox file  
        for axiom in self._tbox_axioms:  
            combined.add_axiom(axiom)  
            
        # 3. Add ABox axioms from an ABox file  
        # Save your ABox graph to a file first
        # with open(abox_path, "w", encoding="utf-8") as f:
        #     f.write(g.serialize(format="xml"))
        for axiom in self.base_ontology.get_abox_axioms():  
            combined.add_axiom(axiom)  
        
        # 4. (Optional) Save the combined ontology  
        # combined.save("src/ontology_req_pipeline/outputs/combined.owl")  
        # 5. Perform reasoning with a chosen OWLAPI reasoner  
        reasoner = None
        try:
            reasoner = SyncReasoner(combined, reasoner=self.reasoner)  # or "Pellet", "ELK", etc.  
            if reasoner.has_consistent_ontology():
                print(f"{self.reasoner} reasoner found the ontology to be consistent.")
            else:
                print(f"{self.reasoner} reasoner found the ontology to be inconsistent.")
            # reasoner.generate_and_save_inferred_class_assertion_axioms("inferred_class_assertions.owl")  
            reasoner.infer_axioms_and_save(
                output_path="src/ontology_req_pipeline/outputs/enriched.owl",
                output_format="ttl",
                inference_types=[
                    "InferredClassAssertionAxiomGenerator", 
                    "InferredSubClassAxiomGenerator", 
                    "InferredDisjointClassesAxiomGenerator", 
                    "InferredEquivalentClassAxiomGenerator", 
                    "InferredEquivalentDataPropertiesAxiomGenerator",
                    "InferredEquivalentObjectPropertyAxiomGenerator", 
                    "InferredInverseObjectPropertiesAxiomGenerator",
                    "InferredSubDataPropertyAxiomGenerator", 
                    "InferredSubObjectPropertyAxiomGenerator",
                    "InferredDataPropertyCharacteristicAxiomGenerator", 
                    "InferredObjectPropertyCharacteristicAxiomGenerator"
                    ],
            )
            inferred_abox = self._materialize_inferred_abox_ontology(combined, reasoner)
            inferred_abox, postprocess_added = self._postprocess_inverse_object_properties(inferred_abox)
            message = f"{self.reasoner} reasoning completed."
            if postprocess_added:
                message += f" RDFLib inverse-property post-processing added {postprocess_added} object property assertion(s)."
            return True, message, inferred_abox, reasoner
        except Exception as exc:  # noqa: BLE001
            return False, f"{self.reasoner} reasoning failed: {exc}", combined, reasoner
