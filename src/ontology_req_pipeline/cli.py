"""Command-line interface for the ontology requirements pipeline."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import statistics
import time
from typing import Any, Dict, List, Optional

import click

from ontology_req_pipeline.extraction.llm_extractor import get_default_extractor
from ontology_req_pipeline.extraction.rule_based_extractor import RuleBasedExtractor
from ontology_req_pipeline.normalization.qudt_normalization import normalize_qudt
from ontology_req_pipeline.ontology.agentic_kg_builder import AgenticKGBuilder
from ontology_req_pipeline.plot_rdf_graph import render_rdf_file_to_png
from dotenv import load_dotenv


DEFAULT_DATASET = Path("datasets/fsae_test_number_unit_sample.jsonl")
DEFAULT_EVALUATION_DIR = Path("src/ontology_req_pipeline/evaluation")
DEFAULT_OPENAI_MODEL = "gpt-5.1"
DEFAULT_OLLAMA_MODEL = "llama3.2"
DEFAULT_COMPARISON_PROFILE = "none"


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _resolve_project_path(path_like: Path) -> Path:
    return path_like if path_like.is_absolute() else (_project_root() / path_like)


def _load_jsonl_rows(path: Path, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as infile:
        for line_no, raw_line in enumerate(infile, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise click.ClickException(f"Invalid JSON at {path}:{line_no}: {exc}") from exc
            if not isinstance(row, dict):
                raise click.ClickException(f"Invalid JSONL row type at {path}:{line_no}. Expected object.")
            rows.append(row)
            if limit is not None and len(rows) >= limit:
                break
    return rows


def _append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8") as outfile:
        outfile.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _first_existing_path(candidates: List[Path]) -> Optional[Path]:
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return None


def _read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            return payload
    except json.JSONDecodeError:
        return {}
    return {}


def _write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        raise click.ClickException(f"Missing JSONL file: {path}")
    return _load_jsonl_rows(path)


def _model_dump_json(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    return value


def _coerce_idx(raw_idx: Any, fallback: int) -> int:
    try:
        return int(raw_idx)
    except (TypeError, ValueError):
        return fallback


def _resolve_stage_config(
    provider: str,
    model: Optional[str],
    provider_option_name: str = "--provider",
) -> Dict[str, Any]:
    normalized_provider = str(provider).strip().lower()
    if normalized_provider not in {"openai", "ollama"}:
        raise click.ClickException(f"{provider_option_name} must be either 'openai' or 'ollama'.")
    selected_model = model.strip() if isinstance(model, str) and model.strip() else (
        DEFAULT_OPENAI_MODEL if normalized_provider == "openai" else DEFAULT_OLLAMA_MODEL
    )
    return {
        "provider": normalized_provider,
        "model": selected_model,
        "local": normalized_provider == "ollama",
    }


def _resolve_extraction_config(provider: str, model: Optional[str]) -> Dict[str, Any]:
    return _resolve_stage_config(provider=provider, model=model, provider_option_name="--provider")


def _resolve_method_choice(method: Optional[str], default: str = "pipeline") -> str:
    raw = str(method or default).strip().lower().replace("_", "-")
    allowed = {
        "pipeline",
        "zero-shot-llm",
        "few-shot-llm",
        "rule-based",
        "quantulum3",
    }
    if raw not in allowed:
        raise click.ClickException(f"Unsupported method: {method}")
    return raw


def _method_prompt_style(method: str) -> str:
    return "zero_shot" if method == "zero-shot-llm" else "few_shot"


def _write_protocol_artifacts(evaluation_dir: Path) -> Dict[str, Any]:
    from ontology_req_pipeline.evaluation.metrics import (
        _load_jsonl as _metrics_load_jsonl,
        _report_markdown,
        _write_jsonl as _metrics_write_jsonl,
        build_claim_ground_truth_template,
        build_extraction_ground_truth_template,
        evaluate_results,
    )

    extraction_rows = _metrics_load_jsonl(evaluation_dir / "extraction.jsonl")
    normalization_rows = _metrics_load_jsonl(evaluation_dir / "normalization.jsonl")
    grounding_rows = _metrics_load_jsonl(evaluation_dir / "grounding.jsonl")

    extraction_gt_path = evaluation_dir / "ground_truth_extraction.jsonl"
    claims_gt_path = evaluation_dir / "ground_truth_claims.jsonl"
    if not extraction_gt_path.exists():
        extraction_template = build_extraction_ground_truth_template(
            extraction_rows=extraction_rows,
            normalization_rows=normalization_rows,
        )
        _metrics_write_jsonl(extraction_gt_path, extraction_template)
    if not claims_gt_path.exists():
        claims_template = build_claim_ground_truth_template(
            extraction_rows=extraction_rows,
            grounding_rows=grounding_rows,
            claims_graph_source="asserted",
        )
        _metrics_write_jsonl(claims_gt_path, claims_template)

    report = evaluate_results(
        evaluation_dir=evaluation_dir,
        extraction_gt_path=extraction_gt_path,
        claims_gt_path=claims_gt_path,
        conformance_graph_source="prefer_inferred",
    )
    report_json_path = evaluation_dir / "evaluation_report.json"
    report_md_path = evaluation_dir / "evaluation_report.md"
    report_json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    report_md_path.write_text(_report_markdown(report), encoding="utf-8")
    return {
        "evaluation_report_json": str(report_json_path.resolve()),
        "evaluation_report_md": str(report_md_path.resolve()),
        "ground_truth_extraction": str(extraction_gt_path.resolve()),
        "ground_truth_claims": str(claims_gt_path.resolve()),
    }


def _run_extraction_for_method(
    extractor,
    extraction_method: str,
    text: str,
    idx: int,
    extraction_config: Dict[str, Any],
):
    if extraction_method == "rule-based":
        return RuleBasedExtractor().extract(text, idx=idx)
    return extractor.extract(
        text,
        local=extraction_config["local"],
        idx=idx,
        model=extraction_config["model"],
        prompt_style=_method_prompt_style(extraction_method),
    )


def _run_normalization_for_method(
    normalization_method: str,
    extracted,
    input_text: str,
    normalization_config: Dict[str, Any],
):
    strategy_map = {
        "pipeline": "pipeline",
        "few-shot-llm": "few_shot_llm",
        "zero-shot-llm": "zero_shot_llm",
        "quantulum3": "quantulum3",
    }
    return normalize_qudt(
        idx=extracted.idx,
        input_text=input_text,
        requirements=extracted.requirements,
        provider=normalization_config["provider"],
        model=normalization_config["model"],
        strategy=strategy_map[normalization_method],
        prompt_style=_method_prompt_style(normalization_method),
    )


def _run_grounding_for_method(
    grounding_method: str,
    normalized,
    grounding_config: Dict[str, Any],
    reasoner: str,
):
    grounder = AgenticKGBuilder(
        tbox_path=_resolve_project_path(Path("ontologies/Core.rdf")),
        record=normalized,
        reasoner=reasoner,
        llm_provider=grounding_config["provider"],
        llm_model=grounding_config["model"],
    )
    if grounding_method == "zero-shot-llm":
        return grounder, grounder.zero_shot_workflow()
    return grounder, grounder.two_stage_workflow()


def _run_raw_grounding_for_method(
    *,
    grounding_method: str,
    idx: int,
    input_text: str,
    grounding_config: Dict[str, Any],
    reasoner: str,
):
    grounder = AgenticKGBuilder(
        tbox_path=_resolve_project_path(Path("ontologies/Core.rdf")),
        record={"idx": idx, "original_text": input_text, "requirements": [], "source": {}},
        reasoner=reasoner,
        llm_provider=grounding_config["provider"],
        llm_model=grounding_config["model"],
    )
    if grounding_method == "zero-shot-llm":
        return grounder, grounder.raw_zero_shot_workflow()
    return grounder, grounder.raw_agentic_workflow()


def _safe_div(numerator: float, denominator: float) -> Optional[float]:
    if denominator == 0:
        return None
    return numerator / denominator


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _percentile(sorted_values: List[float], p: float) -> Optional[float]:
    if not sorted_values:
        return None
    rank = max(0, min(len(sorted_values) - 1, math.ceil(p * len(sorted_values)) - 1))
    return sorted_values[rank]


def _numeric_stats(values: List[float]) -> Dict[str, Optional[float]]:
    cleaned = [float(v) for v in values if v is not None and v >= 0]
    if not cleaned:
        return {
            "count": 0,
            "mean": None,
            "stdev": None,
            "min": None,
            "max": None,
            "p50": None,
            "p95": None,
        }
    cleaned.sort()
    return {
        "count": len(cleaned),
        "mean": statistics.fmean(cleaned),
        "stdev": statistics.pstdev(cleaned) if len(cleaned) > 1 else 0.0,
        "min": cleaned[0],
        "max": cleaned[-1],
        "p50": _percentile(cleaned, 0.50),
        "p95": _percentile(cleaned, 0.95),
    }


def _count_quantity_constraints(record: Dict[str, Any]) -> int:
    total = 0
    for req in record.get("requirements", []):
        for constraint in req.get("constraints", []):
            if constraint.get("value", {}).get("kind") == "quantity":
                total += 1
    return total


def _count_normalized_quantities(record: Dict[str, Any]) -> int:
    total = 0
    for req in record.get("requirements", []):
        total += len(req.get("normalized_quantities", []))
    return total


def _contains_quantity_value(path: Path) -> bool:
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8", errors="ignore")
    return "qudt:QuantityValue" in text or "http://qudt.org/schema/qudt/QuantityValue" in text


def _contains_requirement_markers(path: Path) -> bool:
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8", errors="ignore")
    return "example.org/req/" in text or "VE_req" in text or "Req_0" in text


def _contains_requirement_linkage(path: Path) -> bool:
    if not path.exists():
        return False
    text = path.read_text(encoding="utf-8", errors="ignore")
    has_req_spec = "RequirementSpecification" in text or "iof:RequirementSpecification" in text
    has_sat_link = "requirementSatisfiedBy" in text or "satisfiesRequirement" in text
    return has_req_spec and has_sat_link


def _graph_triple_count(path: Path) -> Optional[int]:
    if not path.exists():
        return None
    try:
        import rdflib  # Imported lazily to keep CLI import resilient.
    except Exception:
        return None

    graph = rdflib.Graph()
    parse_errors: List[str] = []
    for fmt in ("turtle", "xml"):
        try:
            graph.parse(path.as_posix(), format=fmt)
            return len(graph)
        except Exception as exc:  # noqa: BLE001
            parse_errors.append(str(exc))
            graph = rdflib.Graph()
    return None


def _upsert_history_entry(path: Path, entry: Dict[str, Any], key: str = "run_id") -> None:
    existing: List[Dict[str, Any]] = []
    if path.exists():
        for row in _load_jsonl_rows(path):
            if isinstance(row, dict):
                existing.append(row)
    entry_key = entry.get(key)
    filtered = [row for row in existing if row.get(key) != entry_key]
    filtered.append(entry)
    path.write_text("", encoding="utf-8")
    for row in filtered:
        _append_jsonl(path, row)


def _save_requirement_specific_inferred_owl(
    inferred_path: Path,
    final_owl_text: str,
    reasoner_output_path: Path,
) -> Dict[str, Any]:
    """Persist requirement-specific inferred output by merging asserted + inferred graphs.

    Fallback behavior is deterministic: if merge fails, save asserted final graph.
    """
    asserted_ttl = final_owl_text
    mode = "asserted_only_fallback"
    reasoner_output_used = False
    merge_error: Optional[str] = None

    if reasoner_output_path.exists():
        try:
            import rdflib  # Imported lazily to avoid hard dependency at CLI import time.
            from rdflib import BNode, OWL, RDF, RDFS, URIRef
            from collections import deque

            def _extract_requirement_namespace(owl_text: str) -> Optional[str]:
                prefix_match = re.search(r"@prefix\s+:\s*<([^>]+)>", owl_text)
                if prefix_match:
                    return prefix_match.group(1).strip()
                base_match = re.search(r"@base\s+<([^>]+)>", owl_text)
                if base_match:
                    base = base_match.group(1).strip()
                    return base if base.endswith(("#", "/")) else f"{base}#"
                return None

            def _is_schema_triple(subject: Any, predicate: Any, obj: Any) -> bool:
                schema_predicates = {
                    RDFS.subClassOf,
                    RDFS.subPropertyOf,
                    RDFS.domain,
                    RDFS.range,
                    RDFS.seeAlso,
                    RDFS.isDefinedBy,
                    OWL.equivalentClass,
                    OWL.disjointWith,
                    OWL.complementOf,
                    OWL.unionOf,
                    OWL.intersectionOf,
                    OWL.oneOf,
                    OWL.equivalentProperty,
                    OWL.inverseOf,
                    OWL.imports,
                    OWL.versionIRI,
                    OWL.onProperty,
                    OWL.someValuesFrom,
                    OWL.allValuesFrom,
                    OWL.hasValue,
                    OWL.propertyChainAxiom,
                    OWL.sameAs,
                    RDF.first,
                    RDF.rest,
                }
                if predicate in schema_predicates:
                    return True
                schema_type_objects = {
                    OWL.Ontology,
                    OWL.Class,
                    OWL.ObjectProperty,
                    OWL.DatatypeProperty,
                    OWL.AnnotationProperty,
                    OWL.Restriction,
                    OWL.FunctionalProperty,
                    OWL.InverseFunctionalProperty,
                    OWL.SymmetricProperty,
                    OWL.AsymmetricProperty,
                    OWL.TransitiveProperty,
                    OWL.ReflexiveProperty,
                    OWL.IrreflexiveProperty,
                    RDFS.Class,
                    RDF.Property,
                }
                if predicate == RDF.type and obj in schema_type_objects:
                    return True
                # Drop pure vocabulary declarations outside requirement namespace.
                if isinstance(subject, URIRef):
                    subj_str = str(subject)
                    if subj_str.startswith("https://spec.industrialontologies.org/") and predicate == RDF.type:
                        return True
                    if subj_str.startswith("http://purl.obolibrary.org/obo/") and predicate == RDF.type:
                        return True
                    if subj_str.startswith("http://qudt.org/") and predicate == RDF.type:
                        return True
                return False

            def _is_requirement_local_uri(node: Any, requirement_ns: Optional[str]) -> bool:
                if not requirement_ns or not isinstance(node, URIRef):
                    return False
                node_str = str(node)
                if node_str.startswith(requirement_ns):
                    return True
                base = requirement_ns.rstrip("#/")
                return node_str == base

            def _preserve_local_annotations(
                source_graph: "rdflib.Graph",
                target_graph: "rdflib.Graph",
                requirement_ns: Optional[str],
            ) -> None:
                annotation_predicates = {RDFS.label, RDFS.comment, RDFS.seeAlso, RDFS.isDefinedBy}
                local_nodes = set()

                for s, _, o in target_graph:
                    if _is_requirement_local_uri(s, requirement_ns):
                        local_nodes.add(s)
                    if _is_requirement_local_uri(o, requirement_ns):
                        local_nodes.add(o)

                if requirement_ns:
                    local_nodes.add(URIRef(requirement_ns))
                    local_nodes.add(URIRef(requirement_ns.rstrip("#/")))

                for node in local_nodes:
                    for pred in annotation_predicates:
                        for _, _, obj in source_graph.triples((node, pred, None)):
                            target_graph.add((node, pred, obj))

                # Keep the local ontology declaration and its annotations.
                for ont_subject, _, _ in source_graph.triples((None, RDF.type, OWL.Ontology)):
                    if not _is_requirement_local_uri(ont_subject, requirement_ns):
                        continue
                    target_graph.add((ont_subject, RDF.type, OWL.Ontology))
                    for pred in annotation_predicates:
                        for _, _, obj in source_graph.triples((ont_subject, pred, None)):
                            target_graph.add((ont_subject, pred, obj))

            def _build_requirement_connected_abox(graph: "rdflib.Graph", requirement_ns: Optional[str]) -> "rdflib.Graph":
                non_schema_triples: List[Any] = []
                for triple in graph:
                    if not _is_schema_triple(*triple):
                        non_schema_triples.append(triple)

                if not non_schema_triples:
                    return rdflib.Graph()

                outgoing: Dict[Any, List[Any]] = {}
                incoming: Dict[Any, List[Any]] = {}
                for s, p, o in non_schema_triples:
                    outgoing.setdefault(s, []).append((s, p, o))
                    if isinstance(o, (URIRef, BNode)):
                        incoming.setdefault(o, []).append((s, p, o))

                seed_nodes: List[Any] = []
                if requirement_ns:
                    for node in outgoing.keys():
                        if isinstance(node, URIRef) and str(node).startswith(requirement_ns):
                            seed_nodes.append(node)

                if not seed_nodes:
                    for node in outgoing.keys():
                        if isinstance(node, URIRef) and "example.org/req/" in str(node):
                            seed_nodes.append(node)

                # Fallback: keep all non-schema triples if no requirement seeds could be found.
                if not seed_nodes:
                    result = rdflib.Graph()
                    for s, p, o in non_schema_triples:
                        result.add((s, p, o))
                    _preserve_local_annotations(graph, result, requirement_ns=requirement_ns)
                    return result

                keep = rdflib.Graph()
                seen_nodes = set(seed_nodes)
                queue = deque(seed_nodes)

                while queue:
                    node = queue.popleft()
                    for s, p, o in outgoing.get(node, []):
                        keep.add((s, p, o))
                        if isinstance(o, (URIRef, BNode)) and o not in seen_nodes:
                            seen_nodes.add(o)
                            queue.append(o)
                    for s, p, o in incoming.get(node, []):
                        keep.add((s, p, o))
                        if isinstance(s, (URIRef, BNode)) and s not in seen_nodes:
                            seen_nodes.add(s)
                            queue.append(s)

                _preserve_local_annotations(graph, keep, requirement_ns=requirement_ns)
                return keep

            merged = rdflib.Graph()
            merged.parse(data=asserted_ttl, format="turtle")

            # OWLAPI/OWLAPY save() usually emits RDF/XML for .owl outputs.
            # Parsing RDF/XML as Turtle can "succeed" with malformed URI warnings,
            # so detect the likely syntax first and parse accordingly.
            reasoner_text = reasoner_output_path.read_text(encoding="utf-8", errors="ignore")
            reasoner_head = reasoner_text.lstrip()[:256].lower()
            looks_like_xml = reasoner_head.startswith("<?xml") or "<rdf:rdf" in reasoner_head

            if looks_like_xml:
                try:
                    merged.parse(reasoner_output_path.as_posix(), format="xml")
                except Exception:
                    merged.parse(reasoner_output_path.as_posix(), format="turtle")
            else:
                try:
                    merged.parse(reasoner_output_path.as_posix(), format="turtle")
                except Exception:
                    merged.parse(reasoner_output_path.as_posix(), format="xml")

            requirement_ns = _extract_requirement_namespace(asserted_ttl)
            abox_only = _build_requirement_connected_abox(merged, requirement_ns=requirement_ns)
            serialized = abox_only.serialize(format="turtle")
            if isinstance(serialized, bytes):
                serialized = serialized.decode("utf-8")
            inferred_path.write_text(serialized, encoding="utf-8")
            mode = "merged_abox_with_local_annotations"
            reasoner_output_used = True
            return {
                "saved": True,
                "mode": mode,
                "reasoner_output_used": reasoner_output_used,
                "merge_error": merge_error,
            }
        except Exception as exc:  # noqa: BLE001
            merge_error = str(exc)

    inferred_path.write_text(asserted_ttl, encoding="utf-8")
    return {
        "saved": True,
        "mode": mode,
        "reasoner_output_used": reasoner_output_used,
        "merge_error": merge_error,
    }


def _generate_evaluation_qa_report(evaluation_dir: Path) -> Dict[str, Any]:
    extraction_path = evaluation_dir / "extraction.jsonl"
    normalization_path = evaluation_dir / "normalization.jsonl"
    grounding_path = evaluation_dir / "grounding.jsonl"
    run_metadata_path = evaluation_dir / "run_metadata.json"
    qa_history_path = evaluation_dir / "qa_history.jsonl"

    extraction_rows = _read_jsonl(extraction_path)
    normalization_rows = _read_jsonl(normalization_path)
    grounding_rows = _read_jsonl(grounding_path)
    run_metadata = _read_json(run_metadata_path)
    dataset_name = str(
        run_metadata.get("dataset_name")
        or Path(str(run_metadata.get("input_path", ""))).name
        or "unknown"
    )
    extraction_provider = str(
        run_metadata.get("extraction_provider")
        or run_metadata.get("provider")
        or "unknown"
    )
    extraction_model = str(
        run_metadata.get("extraction_model")
        or run_metadata.get("model")
        or "unknown"
    )
    normalization_provider = str(
        run_metadata.get("normalization_provider")
        or extraction_provider
        or "unknown"
    )
    normalization_model = str(
        run_metadata.get("normalization_model")
        or extraction_model
        or "unknown"
    )
    grounding_provider = str(
        run_metadata.get("grounding_provider")
        or extraction_provider
        or "unknown"
    )
    grounding_model = str(
        run_metadata.get("grounding_model")
        or extraction_model
        or "unknown"
    )

    extraction_status = Counter(row.get("status", "unknown") for row in extraction_rows)
    normalization_status = Counter(row.get("status", "unknown") for row in normalization_rows)
    grounding_status = Counter(row.get("status", "unknown") for row in grounding_rows)
    inference_status = Counter(row.get("inference_status", "missing") for row in grounding_rows)

    observed_input_records = len(extraction_rows)
    expected_input_records = _coerce_idx(
        run_metadata.get("input_records_expected"),
        fallback=observed_input_records,
    )
    extraction_ok = extraction_status.get("ok", 0)
    normalization_ok = normalization_status.get("ok", 0)
    grounding_ok = grounding_status.get("ok", 0)
    grounding_failed = grounding_status.get("failed", 0)
    inferred_ok = inference_status.get("ok", 0)

    end_to_end_success = 0
    for row in grounding_rows:
        if row.get("status") != "ok":
            continue
        final_path_raw = row.get("final_kg_path")
        inferred_path_raw = row.get("final_kg_inferred_path")
        if not final_path_raw or not inferred_path_raw:
            continue
        final_path = Path(str(final_path_raw))
        inferred_path = Path(str(inferred_path_raw))
        if final_path.exists() and inferred_path.exists():
            end_to_end_success += 1

    completion_metrics = {
        "observed_input_records": observed_input_records,
        "expected_input_records": expected_input_records,
        "extraction_success_rate_vs_input": _safe_div(extraction_ok, observed_input_records),
        "normalization_success_rate_vs_extraction_ok": _safe_div(normalization_ok, extraction_ok),
        "grounding_success_rate_vs_normalization_ok": _safe_div(grounding_ok, normalization_ok),
        "end_to_end_success_count": end_to_end_success,
        "end_to_end_success_rate_vs_input": _safe_div(end_to_end_success, observed_input_records),
        "end_to_end_success_rate_vs_expected_input": _safe_div(end_to_end_success, expected_input_records),
    }

    total_requirements = 0
    total_constraints = 0
    total_quantity_constraints = 0
    qty_constraints_by_idx: Dict[int, int] = {}

    for row in extraction_rows:
        idx = _coerce_idx(row.get("idx"), fallback=-1)
        record = row.get("record", {})
        qty_count = _count_quantity_constraints(record)
        qty_constraints_by_idx[idx] = qty_count
        for req in record.get("requirements", []):
            total_requirements += 1
            total_constraints += len(req.get("constraints", []))
        total_quantity_constraints += qty_count

    normalization_by_idx = {_coerce_idx(row.get("idx"), fallback=-1): row for row in normalization_rows}
    normalized_quantities_total = 0
    invalid_constraint_idx_total = 0
    normalized_with_unit_total = 0
    valid_constraint_idx_total = 0
    qty_coverage_rows: List[Dict[str, Any]] = []

    for idx, qty_count in sorted(qty_constraints_by_idx.items()):
        norm_row = normalization_by_idx.get(idx, {})
        norm_record = norm_row.get("record", {})
        normalized_qty_count = _count_normalized_quantities(norm_record)
        qty_coverage_rows.append(
            {
                "idx": idx,
                "quantity_constraints_extracted": qty_count,
                "normalized_quantities": normalized_qty_count,
            }
        )

        for req in norm_record.get("requirements", []):
            constraint_count = len(req.get("constraints", []))
            for nq in req.get("normalized_quantities", []):
                normalized_quantities_total += 1
                constraint_idx = nq.get("constraint_idx")
                if not (isinstance(constraint_idx, int) and 0 <= constraint_idx < constraint_count):
                    invalid_constraint_idx_total += 1
                else:
                    valid_constraint_idx_total += 1

                best_unit = str(nq.get("best_unit_uri") or "").strip()
                si_unit = str(nq.get("si_unit_primary") or "").strip()
                if best_unit or si_unit:
                    normalized_with_unit_total += 1

    quant_coverage = (
        _safe_div(normalized_quantities_total, total_quantity_constraints)
    )

    hallucinated_quant_indices: List[int] = []
    non_quant_requirements_with_kg = 0
    for idx, qty_count in qty_constraints_by_idx.items():
        final_kg_path = _first_existing_path(
            [
                evaluation_dir / f"final_kg_{idx}.ttl",
                evaluation_dir / f"final_kg_{idx}.owl",
            ]
        )
        if qty_count == 0 and final_kg_path is not None:
            non_quant_requirements_with_kg += 1
            if _contains_quantity_value(final_kg_path):
                hallucinated_quant_indices.append(idx)

    inferred_files = sorted(
        {
            *evaluation_dir.glob("final_kg_inferred_*.ttl"),
            *evaluation_dir.glob("final_kg_inferred_*.owl"),
        }
    )
    inferred_hashes = Counter(hashlib.sha256(path.read_bytes()).hexdigest() for path in inferred_files)
    inferred_with_req_marker = 0
    for path in inferred_files:
        if _contains_requirement_markers(path):
            inferred_with_req_marker += 1

    grounded_rows_ok = [row for row in grounding_rows if row.get("status") == "ok"]
    grounded_with_requirement_linkage = 0
    for row in grounded_rows_ok:
        final_path_raw = row.get("final_kg_path")
        if final_path_raw and _contains_requirement_linkage(Path(str(final_path_raw))):
            grounded_with_requirement_linkage += 1

    triple_deltas: List[float] = []
    asserted_triple_counts: List[float] = []
    inferred_triple_counts: List[float] = []
    for row in grounded_rows_ok:
        final_path_raw = row.get("final_kg_path")
        inferred_path_raw = row.get("final_kg_inferred_path")
        if not final_path_raw or not inferred_path_raw:
            continue
        final_count = _graph_triple_count(Path(str(final_path_raw)))
        inferred_count = _graph_triple_count(Path(str(inferred_path_raw)))
        if final_count is None or inferred_count is None:
            continue
        asserted_triple_counts.append(float(final_count))
        inferred_triple_counts.append(float(inferred_count))
        triple_deltas.append(float(inferred_count - final_count))

    extraction_seconds = [
        _to_float(row.get("extraction_seconds")) for row in extraction_rows if _to_float(row.get("extraction_seconds")) is not None
    ]
    normalization_seconds = [
        _to_float(row.get("normalization_seconds")) for row in normalization_rows if _to_float(row.get("normalization_seconds")) is not None
    ]
    grounding_seconds = [
        _to_float(row.get("grounding_seconds")) for row in grounding_rows if _to_float(row.get("grounding_seconds")) is not None
    ]
    inference_seconds = [
        _to_float(row.get("inference_seconds")) for row in grounding_rows if _to_float(row.get("inference_seconds")) is not None
    ]
    end_to_end_seconds = [
        _to_float(row.get("record_total_seconds")) for row in grounding_rows if _to_float(row.get("record_total_seconds")) is not None
    ]

    run_duration_seconds = _to_float(run_metadata.get("run_duration_seconds"))
    throughput_records_per_minute = (
        _safe_div(observed_input_records * 60.0, run_duration_seconds) if run_duration_seconds else None
    )

    external_dependency_failure_count = 0
    for row in grounding_rows:
        if row.get("status") != "failed":
            continue
        reason = str(row.get("reason", ""))
        if any(
            token in reason
            for token in [
                "HTTP response code",
                "Could not load imported ontology",
                "UnloadableImportException",
                "OWLOntologyCreationIOException",
                "Connection",
                "timeout",
            ]
        ):
            external_dependency_failure_count += 1

    grounding_attempts = grounding_ok + grounding_failed
    failure_metrics = {
        "grounding_failed_count": grounding_failed,
        "grounding_failed_rate": _safe_div(grounding_failed, grounding_attempts),
        "external_dependency_failure_count": external_dependency_failure_count,
        "external_dependency_failure_rate": _safe_div(external_dependency_failure_count, grounding_attempts),
    }

    quantitative_fidelity = {
        "quantity_coverage": quant_coverage,
        "constraint_index_integrity": _safe_div(valid_constraint_idx_total, normalized_quantities_total),
        "unit_completeness": _safe_div(normalized_with_unit_total, normalized_quantities_total),
        "quantity_hallucination_rate": _safe_div(len(hallucinated_quant_indices), non_quant_requirements_with_kg),
        "quantity_hallucination_count": len(hallucinated_quant_indices),
        "quantity_hallucination_indices": sorted(hallucinated_quant_indices),
    }

    inferred_file_count = len(inferred_files)
    inference_utility = {
        "inferred_file_count": inferred_file_count,
        "inferred_unique_hash_count": len(inferred_hashes),
        "inferred_uniqueness_ratio": _safe_div(len(inferred_hashes), inferred_file_count),
        "inferred_files_with_requirement_markers": inferred_with_req_marker,
        "inferred_marker_presence_rate": _safe_div(inferred_with_req_marker, inferred_file_count),
        "grounded_with_requirement_linkage": grounded_with_requirement_linkage,
        "requirement_linkage_completeness": _safe_div(grounded_with_requirement_linkage, len(grounded_rows_ok)),
        "asserted_triple_stats": _numeric_stats(asserted_triple_counts),
        "inferred_triple_stats": _numeric_stats(inferred_triple_counts),
        "inference_delta_triples_stats": _numeric_stats(triple_deltas),
    }

    call_and_cost = {
        "extraction_invocations": len(extraction_rows),
        "normalization_invocations": len(normalization_rows),
        "grounding_invocations": grounding_attempts,
        "inference_invocations": len(grounded_rows_ok),
        "token_usage_available": False,
        "cost_usage_available": False,
    }

    latency_metrics = {
        "run_duration_seconds": run_duration_seconds,
        "throughput_records_per_minute": throughput_records_per_minute,
        "extraction_seconds": _numeric_stats(extraction_seconds),
        "normalization_seconds": _numeric_stats(normalization_seconds),
        "grounding_seconds": _numeric_stats(grounding_seconds),
        "inference_seconds": _numeric_stats(inference_seconds),
        "end_to_end_seconds": _numeric_stats(end_to_end_seconds),
    }

    run_id = str(run_metadata.get("run_id") or "").strip()
    if not run_id:
        signature = hashlib.sha256()
        for path in (extraction_path, normalization_path, grounding_path):
            if path.exists():
                signature.update(path.read_bytes())
        run_id = f"legacy-{signature.hexdigest()[:16]}"

    history_entry = {
        "run_id": run_id,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "dataset_name": dataset_name,
        "extraction_provider": extraction_provider,
        "extraction_model": extraction_model,
        "normalization_provider": normalization_provider,
        "normalization_model": normalization_model,
        "grounding_provider": grounding_provider,
        "grounding_model": grounding_model,
        "end_to_end_success_rate_vs_input": completion_metrics["end_to_end_success_rate_vs_input"],
        "grounding_failed_rate": failure_metrics["grounding_failed_rate"],
        "external_dependency_failure_rate": failure_metrics["external_dependency_failure_rate"],
        "quantity_hallucination_rate": quantitative_fidelity["quantity_hallucination_rate"],
        "quantity_coverage": quantitative_fidelity["quantity_coverage"],
        "inferred_uniqueness_ratio": inference_utility["inferred_uniqueness_ratio"],
        "run_duration_seconds": latency_metrics["run_duration_seconds"],
    }
    _upsert_history_entry(qa_history_path, history_entry, key="run_id")

    history_rows = _load_jsonl_rows(qa_history_path) if qa_history_path.exists() else []
    robustness_metrics = {
        "runs_in_history": len(history_rows),
        "end_to_end_success_rate_vs_input": _numeric_stats(
            [_to_float(row.get("end_to_end_success_rate_vs_input")) for row in history_rows if _to_float(row.get("end_to_end_success_rate_vs_input")) is not None]
        ),
        "quantity_coverage": _numeric_stats(
            [_to_float(row.get("quantity_coverage")) for row in history_rows if _to_float(row.get("quantity_coverage")) is not None]
        ),
        "quantity_hallucination_rate": _numeric_stats(
            [_to_float(row.get("quantity_hallucination_rate")) for row in history_rows if _to_float(row.get("quantity_hallucination_rate")) is not None]
        ),
        "external_dependency_failure_rate": _numeric_stats(
            [_to_float(row.get("external_dependency_failure_rate")) for row in history_rows if _to_float(row.get("external_dependency_failure_rate")) is not None]
        ),
        "inferred_uniqueness_ratio": _numeric_stats(
            [_to_float(row.get("inferred_uniqueness_ratio")) for row in history_rows if _to_float(row.get("inferred_uniqueness_ratio")) is not None]
        ),
        "run_duration_seconds": _numeric_stats(
            [_to_float(row.get("run_duration_seconds")) for row in history_rows if _to_float(row.get("run_duration_seconds")) is not None]
        ),
    }

    report: Dict[str, Any] = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "evaluation_dir": str(evaluation_dir.resolve()),
        "run_id": run_id,
        "dataset_name": dataset_name,
        "extraction_provider": extraction_provider,
        "extraction_model": extraction_model,
        "normalization_provider": normalization_provider,
        "normalization_model": normalization_model,
        "grounding_provider": grounding_provider,
        "grounding_model": grounding_model,
        "run_metadata": run_metadata,
        "counts": {
            "records_extraction": len(extraction_rows),
            "records_normalization": len(normalization_rows),
            "records_grounding": len(grounding_rows),
            "requirements_extracted": total_requirements,
            "constraints_extracted": total_constraints,
            "quantity_constraints_extracted": total_quantity_constraints,
            "normalized_quantities_total": normalized_quantities_total,
            "normalized_quantities_with_unit": normalized_with_unit_total,
            "invalid_normalized_constraint_idx": invalid_constraint_idx_total,
        },
        "status": {
            "extraction": dict(extraction_status),
            "normalization": dict(normalization_status),
            "grounding": dict(grounding_status),
            "inference": dict(inference_status),
        },
        "completion_metrics": completion_metrics,
        "quantitative_fidelity": quantitative_fidelity,
        "inference_utility": inference_utility,
        "latency_metrics": latency_metrics,
        "call_and_cost": call_and_cost,
        "failure_metrics": failure_metrics,
        "robustness_metrics": robustness_metrics,
        "quality": {
            "quantity_coverage": quantitative_fidelity["quantity_coverage"],
            "quantity_hallucination_count": quantitative_fidelity["quantity_hallucination_count"],
            "quantity_hallucination_indices": quantitative_fidelity["quantity_hallucination_indices"],
            "inferred_file_count": inferred_file_count,
            "inferred_unique_hash_count": inference_utility["inferred_unique_hash_count"],
            "inferred_files_with_requirement_markers": inferred_with_req_marker,
            "inferred_files_without_requirement_markers": inferred_file_count - inferred_with_req_marker,
        },
        "per_record_quantity_coverage": qty_coverage_rows,
    }

    qa_json_path = evaluation_dir / "qa_report.json"
    qa_md_path = evaluation_dir / "qa_report.md"
    qa_json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    coverage_text = f"{quantitative_fidelity['quantity_coverage']:.2%}" if isinstance(quantitative_fidelity["quantity_coverage"], float) else "n/a"
    end_to_end_text = (
        f"{completion_metrics['end_to_end_success_rate_vs_input']:.2%}"
        if isinstance(completion_metrics["end_to_end_success_rate_vs_input"], float)
        else "n/a"
    )
    hallucination_text = (
        f"{quantitative_fidelity['quantity_hallucination_rate']:.2%}"
        if isinstance(quantitative_fidelity["quantity_hallucination_rate"], float)
        else "n/a"
    )
    inferred_uniqueness_text = (
        f"{inference_utility['inferred_uniqueness_ratio']:.2%}"
        if isinstance(inference_utility["inferred_uniqueness_ratio"], float)
        else "n/a"
    )
    md_lines = [
        "# Evaluation QA Report",
        "",
        f"- Generated at (UTC): `{report['generated_at_utc']}`",
        f"- Run ID: `{run_id}`",
        f"- Source dataset: `{dataset_name}`",
        f"- Extraction provider/model: `{extraction_provider}` / `{extraction_model}`",
        f"- Normalization provider/model: `{normalization_provider}` / `{normalization_model}`",
        f"- Grounding provider/model: `{grounding_provider}` / `{grounding_model}`",
        f"- Evaluation directory: `{report['evaluation_dir']}`",
        "",
        "## Summary",
        "",
        f"- Records: extraction={len(extraction_rows)}, normalization={len(normalization_rows)}, grounding={len(grounding_rows)}",
        f"- Requirements extracted: {total_requirements}",
        f"- Constraints extracted: {total_constraints}",
        f"- Quantity constraints extracted: {total_quantity_constraints}",
        f"- Normalized quantities: {normalized_quantities_total}",
        f"- Quantity coverage (normalized/extracted): {coverage_text}",
        f"- End-to-end success rate (vs input): {end_to_end_text}",
        f"- Invalid normalized constraint_idx count: {invalid_constraint_idx_total}",
        f"- Quantity hallucination count/rate: {quantitative_fidelity['quantity_hallucination_count']} / {hallucination_text}",
        f"- Inferred files: {inferred_file_count} (unique hashes: {len(inferred_hashes)}, uniqueness ratio: {inferred_uniqueness_text})",
        (
            f"- Inferred files with requirement markers: {inferred_with_req_marker}/"
            f"{inferred_file_count}"
        ),
        f"- Grounding failed count/rate: {failure_metrics['grounding_failed_count']} / "
        + (f"{failure_metrics['grounding_failed_rate']:.2%}" if isinstance(failure_metrics["grounding_failed_rate"], float) else "n/a"),
        "",
        "## Latency",
        "",
        f"- Run duration (s): {latency_metrics['run_duration_seconds']}",
        f"- Throughput (records/min): {latency_metrics['throughput_records_per_minute']}",
        f"- Extraction p95 (s): {latency_metrics['extraction_seconds']['p95']}",
        f"- Normalization p95 (s): {latency_metrics['normalization_seconds']['p95']}",
        f"- Grounding p95 (s): {latency_metrics['grounding_seconds']['p95']}",
        f"- Inference p95 (s): {latency_metrics['inference_seconds']['p95']}",
        "",
        "## Status Distributions",
        "",
        f"- Extraction: `{dict(extraction_status)}`",
        f"- Normalization: `{dict(normalization_status)}`",
        f"- Grounding: `{dict(grounding_status)}`",
        f"- Inference: `{dict(inference_status)}`",
        "",
        "## Robustness",
        "",
        f"- Runs in history: {robustness_metrics['runs_in_history']}",
        f"- End-to-end success mean (history): {robustness_metrics['end_to_end_success_rate_vs_input']['mean']}",
        f"- Quantity hallucination rate mean (history): {robustness_metrics['quantity_hallucination_rate']['mean']}",
        f"- External dependency failure rate mean (history): {robustness_metrics['external_dependency_failure_rate']['mean']}",
    ]
    qa_md_path.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    return {
        "report": report,
        "qa_json_path": str(qa_json_path.resolve()),
        "qa_md_path": str(qa_md_path.resolve()),
    }

@click.group()
def main() -> None:
    """Ontology-grounded requirements extraction pipeline CLI."""
    
@main.command("run-pipeline")
def run_pipeline() -> None:
    """Run the full ontology requirements extraction pipeline."""
    load_dotenv()  # Load environment variables from .env file
    extractor = get_default_extractor()
    core_ontology_path = _resolve_project_path(Path("ontologies/Core.rdf"))

    record = extractor.extract(
        "A two dimensional template used to represent the 95th percentile male is made to the following dimensions (see figure below): - A circle of diameter 200 mm will represent the hips and buttocks", 
        local=False,
        idx=0,
        )

    print("\n=== Extraction Result ===")
    print(record)

    normalized_record = normalize_qudt(
        idx=record.idx,
        input_text=record.original_text,
        requirements=record.requirements,
    )

    print("\n=== Normalization Result ===")
    print(normalized_record)

    reqAgent = AgenticKGBuilder(
        tbox_path=core_ontology_path,
        record=normalized_record,
        reasoner="Pellet",
    )
    kg_result = reqAgent.two_stage_workflow()
    print("\n=== Final OWL Outputs SAVED ===")
    output_paths = kg_result.get("output_paths", {})
    if output_paths:
        print(f"Initial graph: {output_paths.get('initial_owl')}")
        print(f"Final graph:   {output_paths.get('final_owl')}")
        final_graph_path_raw = output_paths.get("final_owl")
        if final_graph_path_raw:
            final_graph_path = Path(final_graph_path_raw)
            png_path = final_graph_path.with_suffix(".png")
            rendered_png = render_rdf_file_to_png(
                final_graph_path,
                png_path,
                title=f"Ontology Requirement Graph {record.idx}",
            )
            print(f"Graph image:  {rendered_png}")

    # RequirementAgent = Req_Template_Instantiation(tbox_path=str(core_ontology_path), record=normalized_record, reasoner="Pellet")

    # RequirementAgent.save_aboxes()

    # print("\n=== SHACL Validation on Inferred Graph ===")
    # shapes_path = "./src/ontology_req_pipeline/validation/shapes/shacl_qudt.shacl"
    # shapes_graph = rdflib.Graph().parse(shapes_path)

    # kg_inferences = rdflib.Graph().parse("./src/ontology_req_pipeline/outputs/final_kg_inferred.ttl", format="turtle")

    # conforms, report = validate_graph(kg_inferences, shapes_graph)

    # print(f"Conforms: {conforms}")
    # print(report)

@main.command("generate-labeled-dataset")
@click.option(
    "--input-path",
    type=click.Path(path_type=Path),
    default=DEFAULT_DATASET,
    show_default=True,
    help="Path to input JSONL requirement dataset.",
)
@click.option(
    "--output-path",
    type=click.Path(path_type=Path),
    default=DEFAULT_EVALUATION_DIR / "labeled_dataset.jsonl",
    show_default=True,
    help="Path to save extracted labeled records (JSONL).",
)
@click.option(
    "--limit",
    type=int,
    default=10,
    show_default=True,
    help="Number of requirements to process.",
)
@click.option(
    "--provider",
    type=click.Choice(["openai", "ollama"], case_sensitive=False),
    default="openai",
    show_default=True,
    help="Extraction provider to use.",
)
@click.option(
    "--model",
    default=None,
    help=f"Model name for selected provider. Defaults: openai={DEFAULT_OPENAI_MODEL}, ollama={DEFAULT_OLLAMA_MODEL}.",
)
def generate_labeled_dataset(
    input_path: Path,
    output_path: Path,
    limit: int,
    provider: str,
    model: str,
) -> None:
    """Generate a labeled dataset for training."""
    if limit is not None and limit <= 0:
        raise click.ClickException("--limit must be greater than 0.")

    load_dotenv()  # Load environment variables from .env file
    extractor = get_default_extractor()
    extraction_config = _resolve_extraction_config(provider=provider, model=model)

    dataset_path = _resolve_project_path(input_path)
    target_output = _resolve_project_path(output_path)

    if not dataset_path.exists():
        raise click.ClickException(f"Dataset file not found: {dataset_path}")

    rows = _load_jsonl_rows(dataset_path, limit=limit)
    if not rows:
        raise click.ClickException(f"No rows found in dataset: {dataset_path}")

    target_output.parent.mkdir(parents=True, exist_ok=True)
    target_output.write_text("", encoding="utf-8")

    with click.progressbar(rows, label="Generating labeled dataset") as bar:
        for fallback_idx, row in enumerate(bar):
            source_idx = row.get("idx")
            idx = _coerce_idx(source_idx, fallback=fallback_idx)
            text = str(row.get("original_text", "")).strip()
            if not text:
                continue

            record = extractor.extract(
                text,
                local=extraction_config["local"],
                idx=idx,
                model=extraction_config["model"],
            )
            _append_jsonl(target_output, _model_dump_json(record))

    click.echo(
        "Saved labeled dataset to: "
        f"{target_output} (provider={extraction_config['provider']}, model={extraction_config['model']})"
    )


@main.command("run-evaluation-pipeline")
@click.option(
    "--input-path",
    type=click.Path(path_type=Path),
    default=DEFAULT_DATASET,
    show_default=True,
    help="Path to input JSONL requirement dataset.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=DEFAULT_EVALUATION_DIR,
    show_default=True,
    help="Directory where extraction/normalization/grounding outputs are saved.",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    show_default=True,
    help="Number of requirements to process.",
)
@click.option(
    "--provider",
    type=click.Choice(["openai", "ollama"], case_sensitive=False),
    default="openai",
    show_default=True,
    help="Extraction provider to use.",
)
@click.option(
    "--model",
    default=None,
    help=f"Model name for selected provider. Defaults: openai={DEFAULT_OPENAI_MODEL}, ollama={DEFAULT_OLLAMA_MODEL}.",
)
@click.option(
    "--reasoner",
    default="Pellet",
    show_default=True,
    help="Reasoner backend used by grounding.",
)
@click.option(
    "--normalization-provider",
    default=None,
    help="Normalization provider. Defaults to extraction provider when omitted.",
)
@click.option(
    "--normalization-model",
    default=None,
    help="Normalization model for selected normalization provider. Defaults per provider when omitted.",
)
@click.option(
    "--grounding-provider",
    default=None,
    help="Grounding provider. Defaults to extraction provider when omitted.",
)
@click.option(
    "--grounding-model",
    default=None,
    help="Grounding model for selected grounding provider. Defaults per provider when omitted.",
)
@click.option(
    "--comparison-profile",
    type=click.Choice(["none", "fsae-stage-comparison"], case_sensitive=False),
    default=DEFAULT_COMPARISON_PROFILE,
    show_default=True,
    help="Run one configured evaluation or a predefined comparison profile.",
)
@click.option(
    "--extraction-method",
    type=click.Choice(["pipeline", "zero-shot-llm", "rule-based"], case_sensitive=False),
    default="pipeline",
    show_default=True,
    help="Extraction method to evaluate.",
)
@click.option(
    "--normalization-method",
    type=click.Choice(["pipeline", "few-shot-llm", "zero-shot-llm", "quantulum3"], case_sensitive=False),
    default="pipeline",
    show_default=True,
    help="Normalization method to evaluate.",
)
@click.option(
    "--grounding-method",
    type=click.Choice(["pipeline", "zero-shot-llm"], case_sensitive=False),
    default="pipeline",
    show_default=True,
    help="Grounding method to evaluate.",
)
@click.option(
    "--raw-grounding-input/--no-raw-grounding-input",
    default=False,
    show_default=True,
    help="Ground directly from original_text without running extraction or normalization.",
)
def run_evaluation_pipeline(
    input_path: Path,
    output_dir: Path,
    limit: int,
    provider: str,
    model: str,
    reasoner: str,
    normalization_provider: Optional[str],
    normalization_model: Optional[str],
    grounding_provider: Optional[str],
    grounding_model: Optional[str],
    comparison_profile: str,
    extraction_method: str,
    normalization_method: str,
    grounding_method: str,
    raw_grounding_input: bool,
) -> None:
    """Run extraction -> normalization -> grounding and save all outputs."""
    if limit is not None and limit <= 0:
        raise click.ClickException("--limit must be greater than 0.")

    comparison_profile = str(comparison_profile or "none").strip().lower()
    load_dotenv()
    extraction_config = _resolve_extraction_config(provider=provider, model=model)
    extraction_method = _resolve_method_choice(extraction_method, default="pipeline")
    selected_normalization_provider = normalization_provider or extraction_config["provider"]
    selected_normalization_model = (
        normalization_model
        if normalization_model
        else (extraction_config["model"] if selected_normalization_provider == extraction_config["provider"] else None)
    )
    normalization_config = _resolve_stage_config(
        provider=selected_normalization_provider,
        model=selected_normalization_model,
        provider_option_name="--normalization-provider",
    )
    normalization_method = _resolve_method_choice(normalization_method, default="pipeline")
    selected_grounding_provider = grounding_provider or extraction_config["provider"]
    selected_grounding_model = (
        grounding_model
        if grounding_model
        else (extraction_config["model"] if selected_grounding_provider == extraction_config["provider"] else None)
    )
    grounding_config = _resolve_stage_config(
        provider=selected_grounding_provider,
        model=selected_grounding_model,
        provider_option_name="--grounding-provider",
    )
    grounding_method = _resolve_method_choice(grounding_method, default="pipeline")
    raw_grounding_input = bool(raw_grounding_input)
    if raw_grounding_input:
        extraction_method = "raw-requirement"
        normalization_method = "skipped"

    dataset_path = _resolve_project_path(input_path)
    evaluation_dir = _resolve_project_path(output_dir)
    dataset_name = dataset_path.name

    if not dataset_path.exists():
        raise click.ClickException(f"Dataset file not found: {dataset_path}")

    rows = _load_jsonl_rows(dataset_path, limit=limit)
    if not rows:
        raise click.ClickException(f"No rows found in dataset: {dataset_path}")

    if comparison_profile == "fsae-stage-comparison":
        summary = _run_stage_comparison_profile(
            dataset_path=dataset_path,
            evaluation_dir=evaluation_dir,
            rows=rows,
            reasoner=reasoner,
            extraction_config=extraction_config,
            normalization_config=normalization_config,
            grounding_config=grounding_config,
            limit=limit,
            raw_grounding_input=raw_grounding_input,
        )
        click.echo(f"Comparison summary JSON: {summary['summary_json']}")
        click.echo(f"Comparison summary MD:   {summary['summary_md']}")
        return

    extractor = get_default_extractor()

    evaluation_dir.mkdir(parents=True, exist_ok=True)
    run_metadata_path = evaluation_dir / "run_metadata.json"
    run_started_perf = time.perf_counter()
    run_started_utc = datetime.now(timezone.utc).isoformat()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")

    extraction_path = evaluation_dir / "extraction.jsonl"
    normalization_path = evaluation_dir / "normalization.jsonl"
    grounding_path = evaluation_dir / "grounding.jsonl"
    extraction_path.write_text("", encoding="utf-8")
    normalization_path.write_text("", encoding="utf-8")
    grounding_path.write_text("", encoding="utf-8")
    _write_json(
        run_metadata_path,
        {
            "run_id": run_id,
            "started_at_utc": run_started_utc,
            "status": "running",
            "input_path": str(dataset_path.resolve()),
            "dataset_name": dataset_name,
            "output_dir": str(evaluation_dir.resolve()),
            "input_records_expected": len(rows),
            "limit": limit,
            "reasoner": reasoner,
            "extraction_provider": extraction_config["provider"],
            "extraction_local": extraction_config["local"],
            "extraction_model": extraction_config["model"],
            "extraction_method": extraction_method,
            "normalization_provider": normalization_config["provider"],
            "normalization_local": normalization_config["local"],
            "normalization_model": normalization_config["model"],
            "normalization_method": normalization_method,
            "grounding_provider": grounding_config["provider"],
            "grounding_local": grounding_config["local"],
            "grounding_model": grounding_config["model"],
            "grounding_method": grounding_method,
            "raw_grounding_input": raw_grounding_input,
            "provider": extraction_config["provider"],
            "local": extraction_config["local"],
            "model": extraction_config["model"],
        },
    )

    processed = 0
    normalized_ok = 0
    grounded_ok = 0
    grounding_failed = 0
    inferred_ok = 0
    latest_inferred_ontology: Optional[Any] = None

    with click.progressbar(rows, label="Running evaluation pipeline") as bar:
        for fallback_idx, row in enumerate(bar):
            record_started_perf = time.perf_counter()
            source_idx = row.get("idx")
            idx = _coerce_idx(source_idx, fallback=fallback_idx)
            text = str(row.get("original_text", "")).strip()

            if not text:
                _append_jsonl(
                    extraction_path,
                    {
                        "idx": idx,
                        "source_idx": source_idx,
                        "status": "skipped",
                        "reason": "missing_original_text",
                        "extraction_seconds": 0.0,
                        "extraction_method": extraction_method,
                        "normalization_method": normalization_method,
                        "grounding_method": grounding_method,
                    },
                )
                continue

            processed += 1
            if raw_grounding_input:
                _append_jsonl(
                    extraction_path,
                    {
                        "idx": idx,
                        "source_idx": source_idx,
                        "status": "skipped",
                        "reason": "raw_grounding_input",
                        "run_id": run_id,
                        "extraction_provider": extraction_config["provider"],
                        "extraction_model": extraction_config["model"],
                        "extraction_method": extraction_method,
                        "normalization_method": normalization_method,
                        "grounding_method": grounding_method,
                        "provider": extraction_config["provider"],
                        "model": extraction_config["model"],
                        "extraction_seconds": 0.0,
                    },
                )
                _append_jsonl(
                    normalization_path,
                    {
                        "idx": idx,
                        "source_idx": source_idx,
                        "status": "skipped",
                        "reason": "raw_grounding_input",
                        "run_id": run_id,
                        "extraction_provider": extraction_config["provider"],
                        "extraction_model": extraction_config["model"],
                        "extraction_method": extraction_method,
                        "normalization_provider": normalization_config["provider"],
                        "normalization_model": normalization_config["model"],
                        "normalization_method": normalization_method,
                        "grounding_method": grounding_method,
                        "provider": normalization_config["provider"],
                        "model": normalization_config["model"],
                        "normalization_seconds": 0.0,
                    },
                )

                grounding_started = time.perf_counter()
                try:
                    grounder, grounding_result = _run_raw_grounding_for_method(
                        grounding_method=grounding_method,
                        idx=idx,
                        input_text=text,
                        grounding_config=grounding_config,
                        reasoner=reasoner,
                    )
                    grounding_seconds = time.perf_counter() - grounding_started
                except Exception as exc:  # noqa: BLE001
                    grounding_seconds = time.perf_counter() - grounding_started
                    grounding_failed += 1
                    _append_jsonl(
                        grounding_path,
                        {
                            "idx": idx,
                            "source_idx": source_idx,
                            "status": "failed",
                            "reason": f"grounding_failed: {exc}",
                            "error_type": type(exc).__name__,
                            "run_id": run_id,
                            "extraction_provider": extraction_config["provider"],
                            "extraction_model": extraction_config["model"],
                            "extraction_method": extraction_method,
                            "normalization_provider": normalization_config["provider"],
                            "normalization_model": normalization_config["model"],
                            "normalization_method": normalization_method,
                            "grounding_provider": grounding_config["provider"],
                            "grounding_model": grounding_config["model"],
                            "grounding_method": grounding_method,
                            "provider": grounding_config["provider"],
                            "model": grounding_config["model"],
                            "grounding_seconds": grounding_seconds,
                            "inference_status": "skipped",
                            "inference_message": "skipped because grounding failed",
                            "record_total_seconds": time.perf_counter() - record_started_perf,
                        },
                    )
                    continue

                try:
                    final_kg_path = evaluation_dir / f"final_kg_{idx}.ttl"
                    final_kg_path.write_text(
                        grounder.ensure_prefixes(grounding_result["final_owl"]),
                        encoding="utf-8",
                    )

                    inferred_path = evaluation_dir / f"final_kg_inferred_{idx}.ttl"
                    inference_success = False
                    inference_message = ""
                    inference_seconds = None
                    inference_artifact: Dict[str, Any] = {}
                    inference_started = time.perf_counter()
                    try:
                        grounder._update_base_ontology_from_owl(grounding_result["final_owl"])
                        inference_success, inference_message, inferred_ontology, _ = grounder.reason()
                        if inference_success:
                            grounder.inferred_ontology = inferred_ontology
                            latest_inferred_ontology = inferred_ontology
                            reasoner_output_path = _resolve_project_path(
                                Path("src/ontology_req_pipeline/outputs/inferred_abox.owl")
                            )
                            reasoner_output_path.parent.mkdir(parents=True, exist_ok=True)
                            inferred_ontology.save(str(reasoner_output_path))
                            inference_artifact = _save_requirement_specific_inferred_owl(
                                inferred_path=inferred_path,
                                final_owl_text=grounder.ensure_prefixes(grounding_result["final_owl"]),
                                reasoner_output_path=reasoner_output_path,
                            )
                            inferred_ok += 1
                        else:
                            inferred_path.write_text(
                                grounder.ensure_prefixes(grounding_result["final_owl"]),
                                encoding="utf-8",
                            )
                            inference_artifact = {
                                "saved": True,
                                "mode": "asserted_only_fallback",
                                "reasoner_output_used": False,
                                "merge_error": None,
                            }
                        inference_seconds = time.perf_counter() - inference_started
                    except Exception as exc:  # noqa: BLE001
                        inference_message = f"failed to infer final KG: {exc}"
                        inferred_path.write_text(
                            grounder.ensure_prefixes(grounding_result["final_owl"]),
                            encoding="utf-8",
                        )
                        inference_artifact = {
                            "saved": True,
                            "mode": "asserted_only_fallback",
                            "reasoner_output_used": False,
                            "merge_error": inference_message,
                        }
                        inference_seconds = time.perf_counter() - inference_started

                    _append_jsonl(
                        grounding_path,
                        {
                            "idx": idx,
                            "source_idx": source_idx,
                            "status": "ok",
                            "run_id": run_id,
                            "extraction_provider": extraction_config["provider"],
                            "extraction_model": extraction_config["model"],
                            "extraction_method": extraction_method,
                            "normalization_provider": normalization_config["provider"],
                            "normalization_model": normalization_config["model"],
                            "normalization_method": normalization_method,
                            "grounding_provider": grounding_config["provider"],
                            "grounding_model": grounding_config["model"],
                            "grounding_method": grounding_method,
                            "provider": grounding_config["provider"],
                            "model": grounding_config["model"],
                            "final_kg_path": str(final_kg_path.resolve()),
                            "final_kg_inferred_path": (
                                str(inferred_path.resolve()) if inference_artifact.get("saved") else None
                            ),
                            "grounding_seconds": grounding_seconds,
                            "inference_seconds": inference_seconds,
                            "record_total_seconds": time.perf_counter() - record_started_perf,
                            "inference_status": "ok" if inference_success else "failed",
                            "inference_message": inference_message,
                            "inference_artifact": inference_artifact,
                            "output_paths": grounding_result.get("output_paths", {}),
                            "rule_actions": grounding_result.get("rule_actions", []),
                        },
                    )
                    grounded_ok += 1
                except Exception as exc:  # noqa: BLE001
                    grounding_failed += 1
                    _append_jsonl(
                        grounding_path,
                        {
                            "idx": idx,
                            "source_idx": source_idx,
                            "status": "failed",
                            "reason": f"grounding_finalize_failed: {exc}",
                            "error_type": type(exc).__name__,
                            "run_id": run_id,
                            "extraction_provider": extraction_config["provider"],
                            "extraction_model": extraction_config["model"],
                            "extraction_method": extraction_method,
                            "normalization_provider": normalization_config["provider"],
                            "normalization_model": normalization_config["model"],
                            "normalization_method": normalization_method,
                            "grounding_provider": grounding_config["provider"],
                            "grounding_model": grounding_config["model"],
                            "grounding_method": grounding_method,
                            "provider": grounding_config["provider"],
                            "model": grounding_config["model"],
                            "grounding_seconds": grounding_seconds,
                            "inference_status": "skipped",
                            "inference_message": "skipped because finalization failed",
                            "record_total_seconds": time.perf_counter() - record_started_perf,
                        },
                    )
                continue

            extraction_started = time.perf_counter()
            try:
                extracted = _run_extraction_for_method(
                    extractor=extractor,
                    extraction_method=extraction_method,
                    text=text,
                    idx=idx,
                    extraction_config=extraction_config,
                )
                extraction_seconds = time.perf_counter() - extraction_started
                extracted_dump = _model_dump_json(extracted)
                _append_jsonl(
                    extraction_path,
                    {
                        "idx": idx,
                        "source_idx": source_idx,
                        "status": "ok",
                        "run_id": run_id,
                        "extraction_provider": extraction_config["provider"],
                        "extraction_model": extraction_config["model"],
                        "extraction_method": extraction_method,
                        "normalization_method": normalization_method,
                        "grounding_method": grounding_method,
                        "provider": extraction_config["provider"],
                        "model": extraction_config["model"],
                        "extraction_seconds": extraction_seconds,
                        "record": extracted_dump,
                    },
                )
            except Exception as exc:  # noqa: BLE001
                extraction_seconds = time.perf_counter() - extraction_started
                _append_jsonl(
                    extraction_path,
                    {
                        "idx": idx,
                        "source_idx": source_idx,
                        "status": "failed",
                        "reason": f"extraction_failed: {exc}",
                        "error_type": type(exc).__name__,
                        "run_id": run_id,
                        "extraction_provider": extraction_config["provider"],
                        "extraction_model": extraction_config["model"],
                        "extraction_method": extraction_method,
                        "normalization_method": normalization_method,
                        "grounding_method": grounding_method,
                        "provider": extraction_config["provider"],
                        "model": extraction_config["model"],
                        "extraction_seconds": extraction_seconds,
                    },
                )
                _append_jsonl(
                    normalization_path,
                    {
                        "idx": idx,
                        "source_idx": source_idx,
                        "status": "skipped",
                        "reason": "extraction_failed",
                        "run_id": run_id,
                        "extraction_provider": extraction_config["provider"],
                        "extraction_model": extraction_config["model"],
                        "extraction_method": extraction_method,
                        "normalization_provider": normalization_config["provider"],
                        "normalization_model": normalization_config["model"],
                        "normalization_method": normalization_method,
                        "grounding_method": grounding_method,
                        "provider": normalization_config["provider"],
                        "model": normalization_config["model"],
                        "normalization_seconds": 0.0,
                    },
                )
                _append_jsonl(
                    grounding_path,
                    {
                        "idx": idx,
                        "source_idx": source_idx,
                        "status": "skipped",
                        "reason": "extraction_failed",
                        "run_id": run_id,
                        "extraction_provider": extraction_config["provider"],
                        "extraction_model": extraction_config["model"],
                        "extraction_method": extraction_method,
                        "normalization_provider": normalization_config["provider"],
                        "normalization_model": normalization_config["model"],
                        "normalization_method": normalization_method,
                        "grounding_provider": grounding_config["provider"],
                        "grounding_model": grounding_config["model"],
                        "grounding_method": grounding_method,
                        "provider": grounding_config["provider"],
                        "model": grounding_config["model"],
                        "record_total_seconds": time.perf_counter() - record_started_perf,
                    },
                )
                continue

            normalization_started = time.perf_counter()
            try:
                normalized = _run_normalization_for_method(
                    normalization_method=normalization_method,
                    extracted=extracted,
                    input_text=extracted.original_text,
                    normalization_config=normalization_config,
                )
                normalization_seconds = time.perf_counter() - normalization_started
            except Exception as exc:  # noqa: BLE001
                normalization_seconds = time.perf_counter() - normalization_started
                _append_jsonl(
                    normalization_path,
                    {
                        "idx": idx,
                        "source_idx": source_idx,
                        "status": "failed",
                        "reason": f"normalization_failed: {exc}",
                        "error_type": type(exc).__name__,
                        "run_id": run_id,
                        "extraction_provider": extraction_config["provider"],
                        "extraction_model": extraction_config["model"],
                        "extraction_method": extraction_method,
                        "normalization_provider": normalization_config["provider"],
                        "normalization_model": normalization_config["model"],
                        "normalization_method": normalization_method,
                        "grounding_method": grounding_method,
                        "provider": normalization_config["provider"],
                        "model": normalization_config["model"],
                        "normalization_seconds": normalization_seconds,
                    },
                )
                _append_jsonl(
                    grounding_path,
                    {
                        "idx": idx,
                        "source_idx": source_idx,
                        "status": "skipped",
                        "reason": "normalization_failed",
                        "run_id": run_id,
                        "extraction_provider": extraction_config["provider"],
                        "extraction_model": extraction_config["model"],
                        "extraction_method": extraction_method,
                        "normalization_provider": normalization_config["provider"],
                        "normalization_model": normalization_config["model"],
                        "normalization_method": normalization_method,
                        "grounding_provider": grounding_config["provider"],
                        "grounding_model": grounding_config["model"],
                        "grounding_method": grounding_method,
                        "provider": grounding_config["provider"],
                        "model": grounding_config["model"],
                        "record_total_seconds": time.perf_counter() - record_started_perf,
                    },
                )
                continue

            if normalized is None:
                _append_jsonl(
                    normalization_path,
                    {
                        "idx": idx,
                        "source_idx": source_idx,
                        "status": "failed",
                        "reason": "normalize_qudt returned None",
                        "run_id": run_id,
                        "extraction_provider": extraction_config["provider"],
                        "extraction_model": extraction_config["model"],
                        "extraction_method": extraction_method,
                        "normalization_provider": normalization_config["provider"],
                        "normalization_model": normalization_config["model"],
                        "normalization_method": normalization_method,
                        "grounding_method": grounding_method,
                        "provider": normalization_config["provider"],
                        "model": normalization_config["model"],
                        "normalization_seconds": normalization_seconds,
                    },
                )
                _append_jsonl(
                    grounding_path,
                    {
                        "idx": idx,
                        "source_idx": source_idx,
                        "status": "skipped",
                        "reason": "normalization_failed",
                        "run_id": run_id,
                        "extraction_provider": extraction_config["provider"],
                        "extraction_model": extraction_config["model"],
                        "extraction_method": extraction_method,
                        "normalization_provider": normalization_config["provider"],
                        "normalization_model": normalization_config["model"],
                        "normalization_method": normalization_method,
                        "grounding_provider": grounding_config["provider"],
                        "grounding_model": grounding_config["model"],
                        "grounding_method": grounding_method,
                        "provider": grounding_config["provider"],
                        "model": grounding_config["model"],
                        "record_total_seconds": time.perf_counter() - record_started_perf,
                    },
                )
                continue

            normalized_ok += 1
            normalized_dump = _model_dump_json(normalized)
            _append_jsonl(
                normalization_path,
                {
                    "idx": idx,
                    "source_idx": source_idx,
                    "status": "ok",
                    "run_id": run_id,
                    "extraction_provider": extraction_config["provider"],
                    "extraction_model": extraction_config["model"],
                    "extraction_method": extraction_method,
                    "normalization_provider": normalization_config["provider"],
                    "normalization_model": normalization_config["model"],
                    "normalization_method": normalization_method,
                    "grounding_method": grounding_method,
                    "provider": normalization_config["provider"],
                    "model": normalization_config["model"],
                    "normalization_seconds": normalization_seconds,
                    "record": normalized_dump,
                },
            )

            grounding_started = time.perf_counter()
            try:
                grounder, grounding_result = _run_grounding_for_method(
                    grounding_method=grounding_method,
                    normalized=normalized,
                    grounding_config=grounding_config,
                    reasoner=reasoner,
                )
                grounding_seconds = time.perf_counter() - grounding_started
            except Exception as exc:  # noqa: BLE001
                grounding_seconds = time.perf_counter() - grounding_started
                grounding_failed += 1
                _append_jsonl(
                    grounding_path,
                    {
                        "idx": idx,
                        "source_idx": source_idx,
                        "status": "failed",
                        "reason": f"grounding_failed: {exc}",
                        "error_type": type(exc).__name__,
                        "run_id": run_id,
                        "extraction_provider": extraction_config["provider"],
                        "extraction_model": extraction_config["model"],
                        "extraction_method": extraction_method,
                        "normalization_provider": normalization_config["provider"],
                        "normalization_model": normalization_config["model"],
                        "normalization_method": normalization_method,
                        "grounding_provider": grounding_config["provider"],
                        "grounding_model": grounding_config["model"],
                        "grounding_method": grounding_method,
                        "provider": grounding_config["provider"],
                        "model": grounding_config["model"],
                        "grounding_seconds": grounding_seconds,
                        "inference_status": "skipped",
                        "inference_message": "skipped because grounding failed",
                        "record_total_seconds": time.perf_counter() - record_started_perf,
                    },
                )
                continue

            try:
                final_kg_path = evaluation_dir / f"final_kg_{idx}.ttl"
                final_kg_path.write_text(
                    grounder.ensure_prefixes(grounding_result["final_owl"]),
                    encoding="utf-8",
                )

                inferred_path = evaluation_dir / f"final_kg_inferred_{idx}.ttl"
                inference_success = False
                inference_message = ""
                inference_seconds = None
                inference_artifact: Dict[str, Any] = {}
                inference_started = time.perf_counter()
                try:
                    grounder._update_base_ontology_from_owl(grounding_result["final_owl"])
                    inference_success, inference_message, inferred_ontology, _ = grounder.reason()
                    if inference_success:
                        grounder.inferred_ontology = inferred_ontology
                        latest_inferred_ontology = inferred_ontology
                        reasoner_output_path = _resolve_project_path(
                            Path("src/ontology_req_pipeline/outputs/inferred_abox.owl")
                        )
                        reasoner_output_path.parent.mkdir(parents=True, exist_ok=True)
                        inferred_ontology.save(str(reasoner_output_path))
                        inference_artifact = _save_requirement_specific_inferred_owl(
                            inferred_path=inferred_path,
                            final_owl_text=grounder.ensure_prefixes(grounding_result["final_owl"]),
                            reasoner_output_path=reasoner_output_path,
                        )
                        inferred_ok += 1
                    else:
                        inferred_path.write_text(
                            grounder.ensure_prefixes(grounding_result["final_owl"]),
                            encoding="utf-8",
                        )
                        inference_artifact = {
                            "saved": True,
                            "mode": "asserted_only_fallback",
                            "reasoner_output_used": False,
                            "merge_error": None,
                        }
                    inference_seconds = time.perf_counter() - inference_started
                except Exception as exc:  # noqa: BLE001
                    inference_message = f"failed to infer final KG: {exc}"
                    inferred_path.write_text(
                        grounder.ensure_prefixes(grounding_result["final_owl"]),
                        encoding="utf-8",
                    )
                    inference_artifact = {
                        "saved": True,
                        "mode": "asserted_only_fallback",
                        "reasoner_output_used": False,
                        "merge_error": inference_message,
                    }
                    inference_seconds = time.perf_counter() - inference_started

                _append_jsonl(
                    grounding_path,
                    {
                        "idx": idx,
                        "source_idx": source_idx,
                        "status": "ok",
                        "run_id": run_id,
                        "extraction_provider": extraction_config["provider"],
                        "extraction_model": extraction_config["model"],
                        "extraction_method": extraction_method,
                        "normalization_provider": normalization_config["provider"],
                        "normalization_model": normalization_config["model"],
                        "normalization_method": normalization_method,
                        "grounding_provider": grounding_config["provider"],
                        "grounding_model": grounding_config["model"],
                        "grounding_method": grounding_method,
                        "provider": grounding_config["provider"],
                        "model": grounding_config["model"],
                        "final_kg_path": str(final_kg_path.resolve()),
                        "final_kg_inferred_path": (
                            str(inferred_path.resolve()) if inference_artifact.get("saved") else None
                        ),
                        "grounding_seconds": grounding_seconds,
                        "inference_seconds": inference_seconds,
                        "record_total_seconds": time.perf_counter() - record_started_perf,
                        "inference_status": "ok" if inference_success else "failed",
                        "inference_message": inference_message,
                        "inference_artifact": inference_artifact,
                        "output_paths": grounding_result.get("output_paths", {}),
                        "rule_actions": grounding_result.get("rule_actions", []),
                    },
                )
                grounded_ok += 1
            except Exception as exc:  # noqa: BLE001
                grounding_failed += 1
                _append_jsonl(
                    grounding_path,
                    {
                        "idx": idx,
                        "source_idx": source_idx,
                        "status": "failed",
                        "reason": f"grounding_finalize_failed: {exc}",
                        "error_type": type(exc).__name__,
                        "run_id": run_id,
                        "extraction_provider": extraction_config["provider"],
                        "extraction_model": extraction_config["model"],
                        "extraction_method": extraction_method,
                        "normalization_provider": normalization_config["provider"],
                        "normalization_model": normalization_config["model"],
                        "normalization_method": normalization_method,
                        "grounding_provider": grounding_config["provider"],
                        "grounding_model": grounding_config["model"],
                        "grounding_method": grounding_method,
                        "provider": grounding_config["provider"],
                        "model": grounding_config["model"],
                        "grounding_seconds": grounding_seconds,
                        "inference_status": "skipped",
                        "inference_message": "skipped because finalization failed",
                        "record_total_seconds": time.perf_counter() - record_started_perf,
                    },
                )
                continue

    run_duration_seconds = time.perf_counter() - run_started_perf
    _write_json(
        run_metadata_path,
        {
            "run_id": run_id,
            "started_at_utc": run_started_utc,
            "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "completed",
            "input_path": str(dataset_path.resolve()),
            "dataset_name": dataset_name,
            "output_dir": str(evaluation_dir.resolve()),
            "input_records_expected": len(rows),
            "processed_records": processed,
            "normalized_ok": normalized_ok,
            "grounded_ok": grounded_ok,
            "grounding_failed": grounding_failed,
            "inferred_ok": inferred_ok,
            "limit": limit,
            "reasoner": reasoner,
            "extraction_provider": extraction_config["provider"],
            "extraction_local": extraction_config["local"],
            "extraction_model": extraction_config["model"],
            "extraction_method": extraction_method,
            "normalization_provider": normalization_config["provider"],
            "normalization_local": normalization_config["local"],
            "normalization_model": normalization_config["model"],
            "normalization_method": normalization_method,
            "grounding_provider": grounding_config["provider"],
            "grounding_local": grounding_config["local"],
            "grounding_model": grounding_config["model"],
            "grounding_method": grounding_method,
            "raw_grounding_input": raw_grounding_input,
            "provider": extraction_config["provider"],
            "local": extraction_config["local"],
            "model": extraction_config["model"],
            "run_duration_seconds": run_duration_seconds,
        },
    )

    if latest_inferred_ontology is not None:
        latest_inferred_ontology.save(str(evaluation_dir / f"inferred_ontology_{run_id}.owl"))

    qa_report = _generate_evaluation_qa_report(evaluation_dir)
    protocol_artifacts = _write_protocol_artifacts(evaluation_dir)
    report = qa_report["report"]
    click.echo(f"Extraction results:    {extraction_path}")
    click.echo(f"Normalization results: {normalization_path}")
    click.echo(f"Grounding results:     {grounding_path}")
    click.echo(
        f"Processed: {processed}, normalized: {normalized_ok}, grounded: {grounded_ok}, "
        f"grounding_failed: {grounding_failed}, inferred: {inferred_ok}"
    )
    click.echo(
        "Run config: "
        f"dataset={report.get('dataset_name')}, "
        f"extractor={extraction_method}:{report.get('extraction_provider')}/{report.get('extraction_model')}, "
        f"normalizer={normalization_method}:{report.get('normalization_provider')}/{report.get('normalization_model')}, "
        f"grounder={grounding_method}:{report.get('grounding_provider')}/{report.get('grounding_model')}"
    )
    click.echo(f"QA report JSON:        {qa_report['qa_json_path']}")
    click.echo(f"QA report Markdown:    {qa_report['qa_md_path']}")
    click.echo(f"Evaluation report:     {protocol_artifacts['evaluation_report_md']}")


def _write_comparison_summary(output_dir: Path, rows: List[Dict[str, Any]]) -> Dict[str, str]:
    summary_json_path = output_dir / "comparison_summary.json"
    summary_md_path = output_dir / "comparison_summary.md"
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "output_dir": str(output_dir.resolve()),
        "runs": rows,
    }
    summary_json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# Comparison Summary",
        "",
        f"- Generated at (UTC): `{payload['generated_at_utc']}`",
        f"- Output dir: `{payload['output_dir']}`",
        "",
        "| Stage | Method | Run Dir | Quantity Coverage | Grounding Failed | QA Report | Evaluation Report |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| {stage} | {method} | `{run_dir}` | {coverage} | {failed} | `{qa}` | `{report}` |".format(
                stage=row.get("stage"),
                method=row.get("method"),
                run_dir=row.get("run_dir"),
                coverage=row.get("quantity_coverage", "n/a"),
                failed=row.get("grounding_failed_count", "n/a"),
                qa=Path(str(row.get("qa_report_md", ""))).name,
                report=Path(str(row.get("evaluation_report_md", ""))).name,
            )
        )
    summary_md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {
        "summary_json": str(summary_json_path.resolve()),
        "summary_md": str(summary_md_path.resolve()),
    }


def _run_stage_comparison_profile(
    *,
    dataset_path: Path,
    evaluation_dir: Path,
    rows: List[Dict[str, Any]],
    reasoner: str,
    extraction_config: Dict[str, Any],
    normalization_config: Dict[str, Any],
    grounding_config: Dict[str, Any],
    limit: Optional[int],
    raw_grounding_input: bool,
) -> Dict[str, Any]:
    comparison_specs = [
        (
            "grounding-focused",
            "pipeline-extraction__pipeline-normalization__agentic-grounding",
            {
                "extraction_method": "pipeline",
                "normalization_method": "pipeline",
                "grounding_method": "pipeline",
                "raw_grounding_input": False,
            },
        ),
        (
            "grounding-focused",
            "pipeline-extraction__pipeline-normalization__zero-shot-llm-grounding",
            {
                "extraction_method": "pipeline",
                "normalization_method": "pipeline",
                "grounding_method": "zero-shot-llm",
                "raw_grounding_input": False,
            },
        ),
        (
            "grounding-focused",
            "raw-requirement__agentic-grounding",
            {
                "extraction_method": "pipeline",
                "normalization_method": "pipeline",
                "grounding_method": "pipeline",
                "raw_grounding_input": True,
            },
        ),
        (
            "grounding-focused",
            "raw-requirement__zero-shot-llm-grounding",
            {
                "extraction_method": "pipeline",
                "normalization_method": "pipeline",
                "grounding_method": "zero-shot-llm",
                "raw_grounding_input": True,
            },
        ),
    ]

    summary_rows: List[Dict[str, Any]] = []
    evaluation_dir.mkdir(parents=True, exist_ok=True)

    for stage, method, overrides in comparison_specs:
        subdir = evaluation_dir / stage / method.replace("-", "_")
        run_evaluation_pipeline.callback(
            input_path=dataset_path,
            output_dir=subdir,
            limit=limit,
            provider=extraction_config["provider"],
            model=extraction_config["model"],
            reasoner=reasoner,
            normalization_provider=normalization_config["provider"],
            normalization_model=normalization_config["model"],
            grounding_provider=grounding_config["provider"],
            grounding_model=grounding_config["model"],
            comparison_profile="none",
            extraction_method=overrides.get("extraction_method", "pipeline"),
            normalization_method=overrides.get("normalization_method", "pipeline"),
            grounding_method=overrides.get("grounding_method", "pipeline"),
            raw_grounding_input=overrides.get("raw_grounding_input", raw_grounding_input),
        )
        qa_report = _read_json(subdir / "qa_report.json")
        summary_rows.append(
            {
                "stage": stage,
                "method": method,
                "run_dir": str(subdir.resolve()),
                "qa_report_json": str((subdir / "qa_report.json").resolve()),
                "qa_report_md": str((subdir / "qa_report.md").resolve()),
                "evaluation_report_json": str((subdir / "evaluation_report.json").resolve()),
                "evaluation_report_md": str((subdir / "evaluation_report.md").resolve()),
                "ground_truth_extraction": str((subdir / "ground_truth_extraction.jsonl").resolve()),
                "ground_truth_claims": str((subdir / "ground_truth_claims.jsonl").resolve()),
                "quantity_coverage": qa_report.get("quality", {}).get("quantity_coverage"),
                "grounding_failed_count": qa_report.get("failure_metrics", {}).get("grounding_failed_count"),
            }
        )

    return _write_comparison_summary(evaluation_dir, summary_rows)


@main.command("qa-evaluation-report")
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=DEFAULT_EVALUATION_DIR,
    show_default=True,
    help="Directory containing extraction/normalization/grounding artifacts.",
)
def qa_evaluation_report(output_dir: Path) -> None:
    """Compute and save QA metrics for an evaluation output folder."""
    evaluation_dir = _resolve_project_path(output_dir)
    if not evaluation_dir.exists():
        raise click.ClickException(f"Evaluation directory not found: {evaluation_dir}")

    qa_report = _generate_evaluation_qa_report(evaluation_dir)
    quality = qa_report["report"]["quality"]
    report = qa_report["report"]
    click.echo(f"QA report JSON:     {qa_report['qa_json_path']}")
    click.echo(f"QA report Markdown: {qa_report['qa_md_path']}")
    click.echo(
        "Run config: "
        f"dataset={report.get('dataset_name')}, "
        f"extractor={report.get('extraction_provider')}/{report.get('extraction_model')}, "
        f"normalizer={report.get('normalization_provider')}/{report.get('normalization_model')}, "
        f"grounder={report.get('grounding_provider')}/{report.get('grounding_model')}"
    )
    click.echo(
        "Quick quality summary: "
        f"quantity_coverage={quality.get('quantity_coverage')}, "
        f"hallucinations={quality.get('quantity_hallucination_count')}, "
        f"inferred_unique_hashes={quality.get('inferred_unique_hash_count')}/"
        f"{quality.get('inferred_file_count')}"
    )

@main.command("test")
def test() -> None:
    load_dotenv()
    extractor = get_default_extractor()
    record = extractor.extract(
    """The top 180° of the wheels/tires must be unobstructed when viewed from vertically above the wheel.""", 
    local=True,
    idx=0,
    model="gemma3:4b"
    )
    with open("test.json", "w", encoding="utf-8") as f:
        json.dump(record.model_dump(), f, indent=2)


if __name__ == "__main__":
    main()
