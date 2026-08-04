import json
import os
import threading
import warnings
from hashlib import sha256
from pathlib import Path
from typing import Any, Optional

import chromadb
import pandas as pd
from rdflib import Graph, term  # term imported for _toPythonMapping
from rdflib.namespace import RDF

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
    qudt_extraction_wf,
    query_qk_by_unit,
)

# Disable strict parsing of rdf:HTML literals to avoid html5rdf ParseError
if hasattr(RDF, "HTML") and RDF.HTML in term._toPythonMapping:
    # Just return the lexical form as-is (string) instead of parsing as HTML
    term._toPythonMapping[RDF.HTML] = lambda lexical: lexical

MODULE_DIR = Path(__file__).resolve().parent
REPO_ROOT = MODULE_DIR.parents[2]
QUDT_LOCAL_FILE = REPO_ROOT / "ontologies" / "QUDT-all-in-one-OWL.ttl"
QUDT_LOOKUP_CSV = MODULE_DIR / "qudt_quantity_kinds_units_symbols_with_descriptions.csv"
DEFAULT_CHROMA_COLLECTION = "qudt_quantity_kinds_with_descriptions"
DEFAULT_LOCAL_CHROMA_PATH = REPO_ROOT / "artifacts" / "cache" / "qudt-chroma"
LOCAL_INDEX_SCHEMA_VERSION = "1"
LOCAL_INDEX_BATCH_SIZE = 128
LOCAL_INDEX_DOCUMENT_CHAR_LIMIT = 1_000
_COLLECTION_CACHE: dict[tuple[str, ...], Any] = {}
_COLLECTION_LOCK = threading.RLock()
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

def _file_sha256(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _clean_index_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return " ".join(str(value).split())


def _local_index_documents(df: pd.DataFrame) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    for quantity_kind_uri, group in df.groupby("quantity_kind", sort=True, dropna=True):
        uri = _clean_index_text(quantity_kind_uri)
        if not uri:
            continue
        name = uri.rsplit("/", 1)[-1]
        labels = sorted(
            {
                text
                for column in ("quantity_name", "label")
                if column in group
                for text in (_clean_index_text(value) for value in group[column])
                if text
            }
        )
        descriptions = []
        for column in ("description", "description_dcterms", "comment"):
            if column not in group:
                continue
            for value in group[column]:
                text = _clean_index_text(value)
                if text and text not in descriptions:
                    descriptions.append(text)
        units = []
        for _, row in group.iterrows():
            unit = _clean_index_text(row.get("unit"))
            symbol = _clean_index_text(row.get("symbol"))
            display = f"{unit} ({symbol})" if unit and symbol else unit or symbol
            if display and display not in units:
                units.append(display)

        parts = [f"QUDT quantity kind: {name}.", f"URI: {uri}."]
        if labels:
            parts.append(f"Labels: {', '.join(labels[:20])}.")
        if descriptions:
            parts.append(f"Description: {' '.join(descriptions)[:4000]}")
        if units:
            parts.append(f"Applicable units: {', '.join(units[:200])}.")
        documents.append(
            {
                "id": "qk-" + sha256(uri.encode("utf-8")).hexdigest()[:24],
                "document": " ".join(parts)[:LOCAL_INDEX_DOCUMENT_CHAR_LIMIT],
                "metadata": {
                    "quantity_kind_uri": uri,
                    "quantity_kind_name": name,
                    "unit_count": len(units),
                },
            }
        )
    return documents


def _local_chroma_path() -> Path:
    configured = str(os.getenv("QUDT_CHROMA_PATH") or "").strip()
    if not configured:
        return DEFAULT_LOCAL_CHROMA_PATH
    path = Path(configured).expanduser()
    return path if path.is_absolute() else REPO_ROOT / path


def _local_manifest_path(chroma_path: Path, collection_name: str) -> Path:
    collection_key = sha256(collection_name.encode("utf-8")).hexdigest()[:12]
    return chroma_path / f".{collection_key}.qudt-index.json"


def _read_local_manifest(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def _write_local_manifest(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    temporary.replace(path)


def _build_local_qudt_collection(chroma_path: Path, collection_name: str) -> Any:
    chroma_path.mkdir(parents=True, exist_ok=True)
    source_hash = _file_sha256(QUDT_LOOKUP_CSV)
    documents = _local_index_documents(pd.read_csv(QUDT_LOOKUP_CSV))
    manifest_path = _local_manifest_path(chroma_path, collection_name)
    expected_manifest = {
        "schema_version": LOCAL_INDEX_SCHEMA_VERSION,
        "source_sha256": source_hash,
        "document_count": len(documents),
        "collection_name": collection_name,
    }
    client = chromadb.PersistentClient(path=str(chroma_path))
    collection = client.get_or_create_collection(
        name=collection_name,
        metadata={
            "source": "bundled QUDT CSV",
            "schema_version": LOCAL_INDEX_SCHEMA_VERSION,
            "source_sha256": source_hash,
        },
    )
    manifest = _read_local_manifest(manifest_path)
    if manifest == expected_manifest and collection.count() == len(documents):
        return collection

    # This directory is a generated cache. Recreate only the named QUDT
    # collection when its source data or schema changes.
    client.delete_collection(collection_name)
    collection = client.get_or_create_collection(
        name=collection_name,
        metadata={
            "source": "bundled QUDT CSV",
            "schema_version": LOCAL_INDEX_SCHEMA_VERSION,
            "source_sha256": source_hash,
        },
    )
    for start in range(0, len(documents), LOCAL_INDEX_BATCH_SIZE):
        batch = documents[start : start + LOCAL_INDEX_BATCH_SIZE]
        collection.upsert(
            ids=[item["id"] for item in batch],
            documents=[item["document"] for item in batch],
            metadatas=[item["metadata"] for item in batch],
        )
    _write_local_manifest(manifest_path, expected_manifest)
    return collection


def _load_cloud_qudt_collection(collection_name: str) -> Any:
    api_key = str(os.getenv("CHROMA_API_KEY") or "").strip()
    tenant = str(os.getenv("CHROMA_TENANT") or "").strip()
    database = str(os.getenv("CHROMA_DATABASE") or "").strip()
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
            "Cloud QUDT normalization requires Chroma Cloud configuration in the environment. "
            f"Missing required variable(s): {missing}. "
            "Set them in your .env file or use QUDT_CHROMA_BACKEND=local."
        )
    client = chromadb.CloudClient(api_key=api_key, tenant=tenant, database=database)
    return client.get_collection(name=collection_name)


def _load_qudt_collection() -> Any:
    """Load a reusable local QUDT index, or an explicitly configured cloud index."""

    backend = str(os.getenv("QUDT_CHROMA_BACKEND") or "local").strip().lower()
    if backend in {"none", "off", "disabled"}:
        return None
    if backend not in {"local", "cloud", "auto"}:
        raise ValueError("QUDT_CHROMA_BACKEND must be local, cloud, auto, or disabled")

    collection_name = str(os.getenv("CHROMA_COLLECTION") or DEFAULT_CHROMA_COLLECTION).strip()
    cloud_configured = all(
        str(os.getenv(name) or "").strip()
        for name in ("CHROMA_API_KEY", "CHROMA_TENANT", "CHROMA_DATABASE")
    )
    selected_backend = "cloud" if backend == "cloud" or (backend == "auto" and cloud_configured) else "local"
    local_path = _local_chroma_path()
    cache_key = (
        selected_backend,
        collection_name,
        str(local_path.resolve()) if selected_backend == "local" else "cloud",
        str(os.getenv("CHROMA_TENANT") or "") if selected_backend == "cloud" else "",
        str(os.getenv("CHROMA_DATABASE") or "") if selected_backend == "cloud" else "",
    )

    with _COLLECTION_LOCK:
        if cache_key in _COLLECTION_CACHE:
            return _COLLECTION_CACHE[cache_key]
        try:
            if selected_backend == "cloud":
                collection = _load_cloud_qudt_collection(collection_name)
            else:
                collection = _build_local_qudt_collection(local_path, collection_name)
        except EnvironmentError:
            raise
        except Exception as exc:  # noqa: BLE001
            warnings.warn(
                "Could not initialize the "
                f"{selected_backend} Chroma QUDT index; semantic fallback is disabled for this run. "
                f"Deterministic RDF/CSV normalization will continue. Cause: {exc}",
                RuntimeWarning,
                stacklevel=2,
            )
            collection = None
        _COLLECTION_CACHE[cache_key] = collection
        return collection


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
    from ollama import Client as OllamaClient
    from openai import OpenAI

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
    source=None,
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
        source=source or {},
        original_text=input_text,
        requirements=normalized_individual_requirements
    )
