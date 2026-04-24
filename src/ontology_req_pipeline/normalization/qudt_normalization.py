from pathlib import Path
from typing import Any, Optional
import os
from rdflib import Graph, Literal, Namespace
from rdflib.namespace import RDF
from rdflib import term  # for _toPythonMapping

# Disable strict parsing of rdf:HTML literals to avoid html5rdf ParseError
if hasattr(RDF, "HTML") and RDF.HTML in term._toPythonMapping:
    # Just return the lexical form as-is (string) instead of parsing as HTML
    term._toPythonMapping[RDF.HTML] = lambda lexical: lexical

from ontology_req_pipeline.data_models import (
    NormalizedIndividualRequirement,
    NormalizedQuantity,
    NormalizedRecord,
)
from ontology_req_pipeline.normalization.utils import (
    _as_qudt_unit_uri,
    _fallback_from_unit_code,
    _units_for_quantity_kind,
    choose_best_unit,
    convert_to_SI,
    decide_best_qk,
    query_qk_by_unit,
    qudt_extraction_wf,
)
import pandas as pd
import chromadb

MODULE_DIR = Path(__file__).resolve().parent
REPO_ROOT = MODULE_DIR.parents[2]
QUDT_LOCAL_FILE = REPO_ROOT / "ontologies" / "QUDT-all-in-one-OWL.ttl"
QUDT_LOOKUP_CSV = MODULE_DIR / "qudt_quantity_kinds_units_symbols_with_descriptions.csv"
DEFAULT_CHROMA_COLLECTION = "qudt_quantity_kinds_with_descriptions_new"
DEFAULT_QK_BY_UNIT = {
}

def _load_qudt_graph() -> Graph:
    if not QUDT_LOCAL_FILE.exists():
        raise FileNotFoundError(f"QUDT ontology file not found: {QUDT_LOCAL_FILE}")
    g = Graph()
    g.parse(QUDT_LOCAL_FILE, format="turtle")
    print("QUDT graph loaded, triple count:", len(g))
    return g

def _load_qudt_dataframe() -> dict:
    if not QUDT_LOOKUP_CSV.exists():
        raise FileNotFoundError(f"QUDT CSV file not found: {QUDT_LOOKUP_CSV}")
    df = pd.read_csv(QUDT_LOOKUP_CSV)
    return df

def _load_qudt_collection() -> any:
    api_key = str(os.getenv("CHROMA_API_KEY") or "").strip()
    tenant = str(os.getenv("CHROMA_TENANT") or "").strip()
    database = str(os.getenv("CHROMA_DATABASE") or "").strip()
    collection_name = os.getenv("CHROMA_COLLECTION", DEFAULT_CHROMA_COLLECTION)

    missing_vars = [
        name
        for name, value in (
            ("CHROMA_API_KEY", api_key),
            ("CHROMA_TENANT", tenant),
            ("CHROMA_DATABASE", database),
        )
        if not value
    ]
    if missing_vars:
        missing = ", ".join(missing_vars)
        raise EnvironmentError(
            "normalize_qudt() requires Chroma Cloud configuration in the environment. "
            f"Missing required variable(s): {missing}. "
            "Set them in your .env file before running normalization."
        )

    try:
        client = chromadb.CloudClient(
            api_key=api_key,
            tenant=tenant,
            database=database,
        )
        collection = client.get_collection(name=collection_name)
        return collection
    except Exception as exc:  # noqa: BLE001
        print(f"Warning: could not load Chroma QUDT collection; semantic QUDT fallback disabled. Cause: {exc}")
        return None


def _build_constraint_context(requirement_text: str, constraint: Any, fallback_text: str) -> str:
    parts: list[str] = []

    attribute = getattr(constraint, "attribute", None)
    if attribute is not None:
        attribute_name = getattr(attribute, "name", None)
        if attribute_name:
            parts.append(f"attribute: {attribute_name}")

    evidence = getattr(constraint, "evidence", None)
    if evidence is not None:
        evidence_text = getattr(evidence, "text", None)
        if evidence_text:
            parts.append(f"evidence: {evidence_text}")

    value = getattr(constraint, "value", None)
    if value is not None:
        raw_text = getattr(value, "raw_text", None)
        if raw_text:
            parts.append(f"value: {raw_text}")

    # Keep full requirement text as a fallback only, to avoid cross-constraint leakage.
    if not parts and requirement_text:
        parts.append(f"requirement: {requirement_text}")
    if not parts and fallback_text:
        parts.append(f"source: {fallback_text}")

    return " ; ".join(parts)


def _default_quantity_kind_for_unit(unit: str | None) -> Optional[str]:
    unit_uri = _as_qudt_unit_uri(unit)
    if unit_uri is None:
        return None
    return DEFAULT_QK_BY_UNIT.get(str(unit_uri))


def _build_bounds(
    operator: Optional[str],
    si_value_primary: Optional[float],
    si_value_secondary: Optional[float],
    tolerance_si: Optional[float],
) -> dict[str, Optional[float | bool]]:
    if si_value_primary is None:
        return {
            "lower_bound": None,
            "upper_bound": None,
            "lower_bound_included": None,
            "upper_bound_included": None,
        }

    tol = tolerance_si if tolerance_si is not None else 0
    if operator == "gt":
        return {
            "lower_bound": si_value_primary - tol,
            "upper_bound": None,
            "lower_bound_included": False,
            "upper_bound_included": False,
        }
    if operator == "ge":
        return {
            "lower_bound": si_value_primary - tol,
            "upper_bound": None,
            "lower_bound_included": True,
            "upper_bound_included": False,
        }
    if operator == "lt":
        return {
            "lower_bound": None,
            "upper_bound": si_value_primary + tol,
            "lower_bound_included": False,
            "upper_bound_included": False,
        }
    if operator == "le":
        return {
            "lower_bound": None,
            "upper_bound": si_value_primary + tol,
            "lower_bound_included": False,
            "upper_bound_included": True,
        }
    if operator == "between" and si_value_secondary is not None:
        return {
            "lower_bound": min(si_value_primary, si_value_secondary),
            "upper_bound": max(si_value_primary, si_value_secondary),
            "lower_bound_included": True,
            "upper_bound_included": True,
        }
    return {
        "lower_bound": si_value_primary - tol,
        "upper_bound": si_value_primary + tol,
        "lower_bound_included": True,
        "upper_bound_included": True,
    }


def _normalize_constraint_via_candidate_selection(
    constraint_idx: int,
    input_text: str,
    primary_value: float,
    secondary_value: Optional[float],
    tolerance: Optional[float],
    operator: Optional[str],
    unit: str,
    df_qudt,
    g: Graph,
    provider: str,
    model: Optional[str],
    prompt_style: str,
) -> Optional[NormalizedQuantity]:
    from openai import OpenAI
    from ollama import Client as OllamaClient

    client = OpenAI() if provider == "openai" else OllamaClient()
    qk_candidates = query_qk_by_unit(unit, g)
    try:
        qk_values_in_df = set(df_qudt["quantity_kind"].astype(str))
    except Exception:
        qk_values_in_df = set()
    qk_candidates_in_df = [qk for qk in qk_candidates if str(qk) in qk_values_in_df]
    candidate_pool = qk_candidates_in_df if qk_candidates_in_df else qk_candidates

    quantity_kind = None
    extracted_units: list[str] = []
    if candidate_pool:
        quantity_kind = (
            candidate_pool[0]
            if len(candidate_pool) == 1
            else decide_best_qk(
                client,
                input_text,
                candidate_pool,
                provider=provider,
                model=model,
                prompt_style=prompt_style,
            )
        )
        extracted_units = _units_for_quantity_kind(df_qudt, quantity_kind)
    if not extracted_units:
        fallback_qk, fallback_units = _fallback_from_unit_code(df_qudt, unit, preferred_qks=qk_candidates)
        if fallback_qk is not None and fallback_units:
            quantity_kind = fallback_qk
            extracted_units = fallback_units
    if quantity_kind is None or not extracted_units:
        return None

    best_unit = (
        extracted_units[0]
        if len(extracted_units) == 1
        else choose_best_unit(
            client,
            input_text,
            extracted_units,
            quantity_kind,
            provider=provider,
            model=model,
            prompt_style=prompt_style,
        )
    )
    si_value_primary, si_unit_primary = convert_to_SI(primary_value, best_unit, g)
    si_value_secondary = None
    si_unit_secondary = None
    if secondary_value is not None:
        si_value_secondary, si_unit_secondary = convert_to_SI(secondary_value, best_unit, g)

    unit_uri = _as_qudt_unit_uri(best_unit)
    try:
        multiplier = None
        if unit_uri is not None:
            unit_prop = next(
                iter(
                    g.query(
                        """
PREFIX qudt: <http://qudt.org/schema/qudt/>
SELECT ?mult
WHERE {
    OPTIONAL { ?unit qudt:conversionMultiplier ?mult . }
}
""",
                        initBindings={"unit": unit_uri},
                    )
                ),
                None,
            )
            multiplier = float(unit_prop.mult) if unit_prop and getattr(unit_prop, "mult", None) is not None else 1.0
        else:
            multiplier = 1.0
    except Exception:
        multiplier = 1.0
    tolerance_si = float(tolerance) * multiplier if tolerance is not None else None
    bounds = _build_bounds(operator, si_value_primary, si_value_secondary, tolerance_si)
    best_unit_uri = _as_qudt_unit_uri(best_unit)
    return NormalizedQuantity(
        constraint_idx=constraint_idx,
        quantity_kind_uri=str(quantity_kind) if quantity_kind is not None else None,
        best_unit_uri=str(best_unit_uri) if best_unit_uri is not None else str(best_unit),
        si_value_primary=si_value_primary,
        si_unit_primary=str(si_unit_primary) if si_unit_primary is not None else None,
        si_value_secondary=si_value_secondary,
        si_unit_secondary=str(si_unit_secondary) if si_unit_secondary is not None else None,
        lower_bound=bounds["lower_bound"],
        upper_bound=bounds["upper_bound"],
        lower_bound_included=bounds["lower_bound_included"],
        upper_bound_included=bounds["upper_bound_included"],
    )


def _normalize_constraint_via_quantulum3(
    constraint_idx: int,
    input_text: str,
    primary_value: float,
    secondary_value: Optional[float],
    tolerance: Optional[float],
    operator: Optional[str],
    unit: str,
    df_qudt,
    g: Graph,
) -> Optional[NormalizedQuantity]:
    unit_uri = _as_qudt_unit_uri(unit)
    default_qk = _default_quantity_kind_for_unit(unit)
    if unit_uri is None:
        fallback_qk, fallback_units = _fallback_from_unit_code(df_qudt, unit)
        if fallback_qk is None or not fallback_units:
            return None
        quantity_kind = default_qk or fallback_qk
        best_unit = fallback_units[0]
    else:
        qk_candidates = query_qk_by_unit(unit, g)
        if default_qk is not None:
            quantity_kind = default_qk
            best_unit = str(unit_uri)
        elif qk_candidates:
            quantity_kind = qk_candidates[0]
            best_unit = str(unit_uri)
        else:
            fallback_qk, fallback_units = _fallback_from_unit_code(df_qudt, unit)
            if fallback_qk is None or not fallback_units:
                return None
            quantity_kind = default_qk or fallback_qk
            best_unit = fallback_units[0]

    si_value_primary, si_unit_primary = convert_to_SI(primary_value, best_unit, g)
    si_value_secondary = None
    si_unit_secondary = None
    if secondary_value is not None:
        si_value_secondary, si_unit_secondary = convert_to_SI(secondary_value, best_unit, g)
    bounds = _build_bounds(operator, si_value_primary, si_value_secondary, tolerance)
    best_unit_uri = _as_qudt_unit_uri(best_unit)
    return NormalizedQuantity(
        constraint_idx=constraint_idx,
        quantity_kind_uri=str(quantity_kind) if quantity_kind is not None else None,
        best_unit_uri=str(best_unit_uri) if best_unit_uri is not None else str(best_unit),
        si_value_primary=si_value_primary,
        si_unit_primary=str(si_unit_primary) if si_unit_primary is not None else None,
        si_value_secondary=si_value_secondary,
        si_unit_secondary=str(si_unit_secondary) if si_unit_secondary is not None else None,
        lower_bound=bounds["lower_bound"],
        upper_bound=bounds["upper_bound"],
        lower_bound_included=bounds["lower_bound_included"],
        upper_bound_included=bounds["upper_bound_included"],
    )


def normalize_qudt(
    idx,
    input_text,
    requirements,
    provider: str = "openai",
    model: str | None = None,
    strategy: str = "pipeline",
    prompt_style: str = "few_shot",
) -> any:
    g = _load_qudt_graph()
    df_qudt = _load_qudt_dataframe()
    collection = _load_qudt_collection() if strategy == "pipeline" else None

    normalized_individual_requirements = []

    for req in requirements:

        normalized_constraints = []
        for constraint in req.constraints:
            primary_value = None
            secondary_value = None
            tolerance = None
            operator = None
            unit = None

            if constraint.value is None or constraint.value.kind != "quantity":
                continue

            q = constraint.value.quantity
            if q is None:
                continue

            # Prefer the constraint that actually carries the primary value (e.g., eq/gt/le).
            if q.v1 is not None:
                primary_value = q.v1
                secondary_value = q.v2
                operator = constraint.operator

            # Capture tolerance if provided.
            if q.tol is not None:
                tolerance = q.tol

            # Keep the last non-empty unit_text we see.
            if q.unit_text:
                unit = q.unit_text

            if unit is None:
                # Skip this quantity constraint when extraction did not provide a unit.
                continue
            if primary_value is None:
                # Without a primary numeric value we cannot normalize; skip this constraint.
                continue

            constraint_context = _build_constraint_context(
                requirement_text=req.raw_text,
                constraint=constraint,
                fallback_text=input_text,
            )

            normalized_constraint = qudt_extraction_wf(
                constraint.constraint_idx,
                constraint_context,
                primary_value,
                secondary_value,
                tolerance,
                operator,
                unit,
                df_qudt,
                g,
                collection,
                provider=provider,
                model=model,
                prompt_style=prompt_style,
            ) if strategy == "pipeline" else None
            if strategy == "few_shot_llm":
                normalized_constraint = _normalize_constraint_via_candidate_selection(
                    constraint.constraint_idx,
                    constraint_context,
                    primary_value,
                    secondary_value,
                    tolerance,
                    operator,
                    unit,
                    df_qudt,
                    g,
                    provider=provider,
                    model=model,
                    prompt_style="few_shot",
                )
            elif strategy == "zero_shot_llm":
                normalized_constraint = _normalize_constraint_via_candidate_selection(
                    constraint.constraint_idx,
                    constraint_context,
                    primary_value,
                    secondary_value,
                    tolerance,
                    operator,
                    unit,
                    df_qudt,
                    g,
                    provider=provider,
                    model=model,
                    prompt_style="zero_shot",
                )
            elif strategy == "quantulum3":
                normalized_constraint = _normalize_constraint_via_quantulum3(
                    constraint.constraint_idx,
                    constraint_context,
                    primary_value,
                    secondary_value,
                    tolerance,
                    operator,
                    unit,
                    df_qudt,
                    g,
                )
            if normalized_constraint is not None:
                normalized_constraints.append(normalized_constraint)

        normalized_individual_requirements.append(
            NormalizedIndividualRequirement(
                req_idx=req.req_idx,
                structure=req.structure,
                constraints=req.constraints,
                references=req.references,
                raw_text=req.raw_text,
                normalized_quantities=normalized_constraints
            )
        )
    

    return NormalizedRecord(
        idx=idx,
        original_text=input_text,
        requirements=normalized_individual_requirements
    )
