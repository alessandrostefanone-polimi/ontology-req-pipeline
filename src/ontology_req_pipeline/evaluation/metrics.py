
"""Evaluation utilities for extraction, normalization, and KG outputs.

This module does three things:
1. Generates editable ground-truth templates from current pipeline outputs.
2. Computes formal metrics from filled ground-truth files.
3. Runs SPARQL-based conformance checks over produced KG files.

Usage:
    python -m ontology_req_pipeline.evaluation.metrics --evaluation-dir src/ontology_req_pipeline/evaluation --init-ground-truth
    python -m ontology_req_pipeline.evaluation.metrics --evaluation-dir src/ontology_req_pipeline/evaluation
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import rdflib
from rdflib import Literal, URIRef
from rdflib.namespace import OWL, RDF, RDFS


IOF = "https://spec.industrialontologies.org/ontology/core/Core/"
QUDT = "http://qudt.org/schema/qudt/"

DEFAULT_EXTRACTION_GT = "ground_truth_extraction.jsonl"
DEFAULT_CLAIMS_GT = "ground_truth_claims.jsonl"
DEFAULT_REPORT_JSON = "evaluation_report.json"
DEFAULT_REPORT_MD = "evaluation_report.md"


SPARQL_PREFIXES = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX iof: <https://spec.industrialontologies.org/ontology/core/Core/>
PREFIX qudt: <http://qudt.org/schema/qudt/>
""".strip()


@dataclass(frozen=True)
class ConformanceCheck:
    """One SPARQL conformance check."""

    check_id: str
    title: str
    severity: str
    query: str


CONFORMANCE_CHECKS: List[ConformanceCheck] = [
    ConformanceCheck(
        check_id="C01",
        title="Requirement has iof:RequirementSpecification type",
        severity="error",
        query=SPARQL_PREFIXES
        + """
SELECT ?req ?spec WHERE {
  ?req iof:requirementSatisfiedBy ?spec .
  FILTER NOT EXISTS { ?req a iof:RequirementSpecification . }
}
""",
    ),
    ConformanceCheck(
        check_id="C02",
        title="RequirementSpecification has satisfaction link",
        severity="error",
        query=SPARQL_PREFIXES
        + """
SELECT ?req WHERE {
  ?req a iof:RequirementSpecification .
  FILTER NOT EXISTS { ?req iof:requirementSatisfiedBy ?spec . }
}
""",
    ),
    ConformanceCheck(
        check_id="C03",
        title="requirementSatisfiedBy target typed as DesignSpecification or PlanSpecification",
        severity="error",
        query=SPARQL_PREFIXES
        + """
SELECT ?req ?spec WHERE {
  ?req iof:requirementSatisfiedBy ?spec .
  FILTER NOT EXISTS {
    { ?spec a iof:DesignSpecification . }
    UNION
    { ?spec a iof:PlanSpecification . }
  }
}
""",
    ),
    ConformanceCheck(
        check_id="C04",
        title="QuantityValue must have qudt:unit",
        severity="error",
        query=SPARQL_PREFIXES
        + """
SELECT ?value WHERE {
  ?value a qudt:QuantityValue .
  FILTER NOT EXISTS { ?value qudt:unit ?unit . }
}
""",
    ),
    ConformanceCheck(
        check_id="C05",
        title="Unit used by QuantityValue must declare qudt:hasQuantityKind",
        severity="error",
        query=SPARQL_PREFIXES
        + """
SELECT ?value ?unit WHERE {
  ?value a qudt:QuantityValue ;
         qudt:unit ?unit .
  FILTER NOT EXISTS { ?unit qudt:hasQuantityKind ?qk . }
}
""",
    ),
    ConformanceCheck(
        check_id="C06",
        title="QuantityValue must link to entity via isValueExpressionOf*",
        severity="error",
        query=SPARQL_PREFIXES
        + """
SELECT ?value WHERE {
  ?value a qudt:QuantityValue .
  FILTER NOT EXISTS {
    { ?value iof:isValueExpressionOfAtSomeTime ?entity . }
    UNION
    { ?value iof:isValueExpressionOfAtAllTimes ?entity . }
  }
}
""",
    ),
]


def _safe_div(numerator: float, denominator: float) -> Optional[float]:
    if denominator == 0:
        return None
    return numerator / denominator


def _round_or_none(value: Optional[float], digits: int = 6) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as infile:
        for line_no, raw_line in enumerate(infile, start=1):
            line = raw_line.strip()
            if not line:
                continue
            payload = json.loads(line)
            if not isinstance(payload, dict):
                raise ValueError(f"Expected object in {path}:{line_no}")
            rows.append(payload)
    return rows


def _write_jsonl(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as outfile:
        for row in rows:
            outfile.write(json.dumps(row, ensure_ascii=False) + "\n")


def _to_int(value: Any, fallback: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


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


def _build_normalization_lookup(
    normalization_rows: Sequence[Dict[str, Any]],
) -> Dict[Tuple[int, int, int], Dict[str, Any]]:
    lookup: Dict[Tuple[int, int, int], Dict[str, Any]] = {}
    for row in normalization_rows:
        idx = _to_int(row.get("idx"))
        record = row.get("record", {})
        for req in record.get("requirements", []):
            req_idx = _to_int(req.get("req_idx"))
            for normalized in req.get("normalized_quantities", []):
                constraint_idx = _to_int(normalized.get("constraint_idx"))
                lookup[(idx, req_idx, constraint_idx)] = normalized
    return lookup

def build_extraction_ground_truth_template(
    extraction_rows: Sequence[Dict[str, Any]],
    normalization_rows: Sequence[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Build template for manual extraction/normalization annotation."""
    normalization_lookup = _build_normalization_lookup(normalization_rows)
    template: List[Dict[str, Any]] = []

    for row in extraction_rows:
        if row.get("status") != "ok":
            continue
        idx = _to_int(row.get("idx"))
        record = row.get("record", {})
        original_text = str(record.get("original_text", ""))
        for req in record.get("requirements", []):
            req_idx = _to_int(req.get("req_idx"))
            structure = req.get("structure", {})
            quantity_constraints: List[Dict[str, Any]] = []
            for constraint in req.get("constraints", []):
                value = constraint.get("value", {})
                if value.get("kind") != "quantity":
                    continue

                constraint_idx = _to_int(constraint.get("constraint_idx"))
                quantity = value.get("quantity", {}) or {}
                normalized = normalization_lookup.get((idx, req_idx, constraint_idx), {})

                quantity_constraints.append(
                    {
                        "constraint_idx": constraint_idx,
                        "attribute_name_pred": constraint.get("attribute", {}).get("name"),
                        "operator_pred": constraint.get("operator"),
                        "value_raw_pred": value.get("raw_text"),
                        "value_v1_pred": quantity.get("v1"),
                        "value_v2_pred": quantity.get("v2"),
                        "unit_text_pred": quantity.get("unit_text"),
                        "normalized_quantity_kind_uri_pred": normalized.get("quantity_kind_uri"),
                        "normalized_unit_uri_pred": normalized.get("best_unit_uri"),
                        "normalized_si_value_primary_pred": normalized.get("si_value_primary"),
                        "normalized_si_unit_primary_pred": normalized.get("si_unit_primary"),
                        "labels": {
                            "is_true_quantitative_constraint": None,
                            "operator_correct": None,
                            "quantity_value_correct": None,
                            "unit_correct": None,
                            "quantity_kind_correct": None,
                            "equivalence_correct": None,
                        },
                    }
                )

            template.append(
                {
                    "idx": idx,
                    "req_idx": req_idx,
                    "source_text": original_text,
                    "requirement_text_pred": req.get("raw_text", ""),
                    "prediction": {
                        "subject": structure.get("subject", {}).get("text"),
                        "modality": structure.get("modality"),
                        "condition": structure.get("condition", {}).get("text"),
                        "action": structure.get("action", {}).get("text"),
                        "object": structure.get("object", {}).get("text"),
                    },
                    "labels": {
                        "decomposition_error": None,
                        "subject_correct": None,
                        "modality_correct": None,
                        "condition_correct": None,
                        "action_correct": None,
                        "object_correct": None,
                    },
                    "quantity_constraints": quantity_constraints,
                    "missing_quantitative_constraints": [],
                    "notes": "",
                }
            )

    template.sort(key=lambda row: (row.get("idx", -1), row.get("req_idx", -1)))
    return template


def _term_to_json(term: Any) -> str:
    if isinstance(term, Literal):
        return term.n3()
    return str(term)


def _is_schema_triple(subject: Any, predicate: Any, obj: Any) -> bool:
    schema_predicates = {
        RDFS.subClassOf,
        RDFS.subPropertyOf,
        RDFS.domain,
        RDFS.range,
        OWL.equivalentClass,
        OWL.equivalentProperty,
        OWL.disjointWith,
        OWL.inverseOf,
        OWL.imports,
        OWL.onProperty,
        OWL.someValuesFrom,
        OWL.allValuesFrom,
        OWL.hasValue,
        RDF.first,
        RDF.rest,
    }
    if predicate in schema_predicates:
        return True

    if predicate in {RDFS.label, RDFS.comment}:
        return True

    if predicate == RDF.type:
        schema_type_objects = {
            OWL.Ontology,
            OWL.Class,
            OWL.ObjectProperty,
            OWL.DatatypeProperty,
            OWL.AnnotationProperty,
            OWL.Restriction,
            OWL.NamedIndividual,
            OWL.Thing,
            RDF.Property,
            RDFS.Class,
        }
        if obj in schema_type_objects:
            return True
        if isinstance(obj, URIRef):
            obj_str = str(obj)
            if obj_str.startswith("http://purl.obolibrary.org/obo/BFO_"):
                return True

    if isinstance(subject, URIRef) and str(subject).startswith("https://spec.industrialontologies.org/"):
        return True
    return False


def _extract_claims(path: Path) -> List[Dict[str, str]]:
    graph = rdflib.Graph()
    graph.parse(path.as_posix(), format="turtle")
    claims: List[Dict[str, str]] = []
    for subject, predicate, obj in graph:
        if _is_schema_triple(subject, predicate, obj):
            continue
        claims.append(
            {
                "s": _term_to_json(subject),
                "p": _term_to_json(predicate),
                "o": _term_to_json(obj),
                "supported_by_sentence": None,
                "notes": "",
            }
        )
    claims.sort(key=lambda row: (row["s"], row["p"], row["o"]))
    return claims


def build_claim_ground_truth_template(
    extraction_rows: Sequence[Dict[str, Any]],
    grounding_rows: Sequence[Dict[str, Any]],
    claims_graph_source: str,
) -> List[Dict[str, Any]]:
    """Build template for TP/FP/FN claim-level annotation."""
    extraction_text_by_idx: Dict[int, str] = {}
    for row in extraction_rows:
        idx = _to_int(row.get("idx"))
        record = row.get("record", {})
        extraction_text_by_idx[idx] = str(record.get("original_text", ""))

    template: List[Dict[str, Any]] = []
    for row in grounding_rows:
        if row.get("status") != "ok":
            continue
        idx = _to_int(row.get("idx"))
        asserted_path = Path(str(row.get("final_kg_path", ""))) if row.get("final_kg_path") else None
        inferred_path = (
            Path(str(row.get("final_kg_inferred_path", ""))) if row.get("final_kg_inferred_path") else None
        )

        graph_path: Optional[Path] = None
        if claims_graph_source == "asserted":
            graph_path = asserted_path
        elif claims_graph_source == "inferred":
            graph_path = inferred_path
        else:
            graph_path = inferred_path if inferred_path and inferred_path.exists() else asserted_path

        if not graph_path or not graph_path.exists():
            continue

        claims = _extract_claims(graph_path)
        template.append(
            {
                "idx": idx,
                "source_text": extraction_text_by_idx.get(idx, ""),
                "graph_path": str(graph_path),
                "predicted_claims": claims,
                "missing_gold_claims": [],
                "notes": "",
            }
        )

    template.sort(key=lambda row: row.get("idx", -1))
    return template


def _bool_accuracy(values: Iterable[Any]) -> Dict[str, Optional[float]]:
    judged = [value for value in values if isinstance(value, bool)]
    correct = sum(1 for value in judged if value)
    return {
        "correct": correct,
        "judged": len(judged),
        "accuracy": _round_or_none(_safe_div(correct, len(judged))),
    }


def _is_non_empty_claim(claim: Any) -> bool:
    if not isinstance(claim, dict):
        return False
    return all(str(claim.get(field, "")).strip() for field in ("s", "p", "o"))

def compute_ground_truth_metrics(
    extraction_gt_rows: Sequence[Dict[str, Any]],
    claims_gt_rows: Sequence[Dict[str, Any]],
    extraction_rows: Sequence[Dict[str, Any]],
    normalization_rows: Sequence[Dict[str, Any]],
) -> Dict[str, Any]:
    """Compute publication-facing metrics from filled ground-truth annotations."""
    structure_by_slot: Dict[str, List[Any]] = {
        "subject": [],
        "modality": [],
        "condition": [],
        "action": [],
        "object": [],
    }
    conditional_structure_by_slot: Dict[str, List[Any]] = {
        "subject": [],
        "modality": [],
        "condition": [],
        "action": [],
        "object": [],
    }
    decomposition_values: List[Any] = []
    decomposition_ok_rows = 0

    extracted_qty_total = 0
    qty_judged = 0
    qty_true = 0
    missing_qty_total = 0
    operator_values: List[Any] = []
    quantity_values: List[Any] = []
    unit_values: List[Any] = []
    quantity_kind_values: List[Any] = []
    equivalence_values: List[Any] = []

    for row in extraction_gt_rows:
        labels = row.get("labels", {})
        decomposition_value = labels.get("decomposition_error")
        decomposition_values.append(decomposition_value)
        for slot in structure_by_slot:
            slot_value = labels.get(f"{slot}_correct")
            structure_by_slot[slot].append(slot_value)
            if decomposition_value is False:
                conditional_structure_by_slot[slot].append(slot_value)
        if decomposition_value is False:
            decomposition_ok_rows += 1

        missing_constraints = row.get("missing_quantitative_constraints", [])
        missing_qty_total += sum(1 for item in missing_constraints if isinstance(item, dict))

        for constraint in row.get("quantity_constraints", []):
            extracted_qty_total += 1
            c_labels = constraint.get("labels", {})
            is_true = c_labels.get("is_true_quantitative_constraint")
            if isinstance(is_true, bool):
                qty_judged += 1
                if is_true:
                    qty_true += 1
                    operator_values.append(c_labels.get("operator_correct"))
                    quantity_values.append(c_labels.get("quantity_value_correct"))
                    unit_values.append(c_labels.get("unit_correct"))
                    quantity_kind_values.append(c_labels.get("quantity_kind_correct"))
                    equivalence_values.append(c_labels.get("equivalence_correct"))

    structure_metrics = {
        slot: _bool_accuracy(values) for slot, values in structure_by_slot.items()
    }
    conditional_structure_metrics = {
        slot: _bool_accuracy(values) for slot, values in conditional_structure_by_slot.items()
    }
    decomposition_judged = sum(1 for value in decomposition_values if isinstance(value, bool))
    decomposition_errors = sum(1 for value in decomposition_values if value is True)
    decomposition_ok = sum(1 for value in decomposition_values if value is False)
    decomposition_error_rate = _round_or_none(_safe_div(decomposition_errors, decomposition_judged))
    decomposition_accuracy = _round_or_none(_safe_div(decomposition_ok, decomposition_judged))
    judged_slot_accuracies = [
        metric["accuracy"]
        for metric in structure_metrics.values()
        if metric.get("accuracy") is not None
    ]
    structure_macro_accuracy = (
        _round_or_none(sum(judged_slot_accuracies) / len(judged_slot_accuracies))
        if judged_slot_accuracies
        else None
    )
    conditional_judged_slot_accuracies = [
        metric["accuracy"]
        for metric in conditional_structure_metrics.values()
        if metric.get("accuracy") is not None
    ]
    conditional_structure_macro_accuracy = (
        _round_or_none(sum(conditional_judged_slot_accuracies) / len(conditional_judged_slot_accuracies))
        if conditional_judged_slot_accuracies
        else None
    )

    gold_qty_total = qty_true + missing_qty_total
    quantity_constraint_metrics = {
        "extracted_quantitative_constraints": extracted_qty_total,
        "judged_extracted_quantitative_constraints": qty_judged,
        "correct_extracted_quantitative_constraints": qty_true,
        "missing_quantitative_constraints": missing_qty_total,
        "gold_quantitative_constraints_total": gold_qty_total,
        "quantitative_constraint_precision": _round_or_none(_safe_div(qty_true, qty_judged)),
        "quantitative_constraint_recall": _round_or_none(_safe_div(qty_true, gold_qty_total)),
        "operator_accuracy": _bool_accuracy(operator_values),
        "quantity_value_accuracy": _bool_accuracy(quantity_values),
        "unit_accuracy": _bool_accuracy(unit_values),
        "quantity_kind_accuracy": _bool_accuracy(quantity_kind_values),
        "equivalence_accuracy": _bool_accuracy(equivalence_values),
    }

    extracted_quantities_from_pipeline = 0
    normalized_quantities_from_pipeline = 0
    for row in extraction_rows:
        if row.get("status") == "ok":
            extracted_quantities_from_pipeline += _count_quantity_constraints(row.get("record", {}))
    for row in normalization_rows:
        if row.get("status") == "ok":
            normalized_quantities_from_pipeline += _count_normalized_quantities(row.get("record", {}))

    normalization_metrics = {
        "normalized_quantities": normalized_quantities_from_pipeline,
        "extracted_quantities": extracted_quantities_from_pipeline,
        "normalized_over_extracted": _round_or_none(
            _safe_div(normalized_quantities_from_pipeline, extracted_quantities_from_pipeline)
        ),
        "correct_quantity_kinds_over_judged": quantity_constraint_metrics["quantity_kind_accuracy"],
        "correct_units_over_judged": quantity_constraint_metrics["unit_accuracy"],
        "correct_equivalences_over_judged": quantity_constraint_metrics["equivalence_accuracy"],
    }

    tp = 0
    fp = 0
    annotated_predicted_claims = 0
    total_predicted_claims = 0
    fn = 0

    for row in claims_gt_rows:
        for claim in row.get("predicted_claims", []):
            total_predicted_claims += 1
            support = claim.get("supported_by_sentence")
            if isinstance(support, bool):
                annotated_predicted_claims += 1
                if support:
                    tp += 1
                else:
                    fp += 1

        for claim in row.get("missing_gold_claims", []):
            if _is_non_empty_claim(claim):
                fn += 1

    precision = _safe_div(tp, tp + fp)
    recall = _safe_div(tp, tp + fn)
    f1 = _safe_div(2 * precision * recall, precision + recall) if precision is not None and recall is not None else None

    claim_metrics = {
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": None,
        "precision": _round_or_none(precision),
        "recall": _round_or_none(recall),
        "f1": _round_or_none(f1),
        "annotated_predicted_claims": annotated_predicted_claims,
        "total_predicted_claims": total_predicted_claims,
        "predicted_claim_annotation_coverage": _round_or_none(
            _safe_div(annotated_predicted_claims, total_predicted_claims)
        ),
    }

    return {
        "decomposition_metrics": {
            "judged": decomposition_judged,
            "errors": decomposition_errors,
            "correct": decomposition_ok,
            "error_rate": decomposition_error_rate,
            "accuracy": decomposition_accuracy,
        },
        "structure_metrics": structure_metrics,
        "structure_macro_accuracy": structure_macro_accuracy,
        "conditional_structure_metrics": conditional_structure_metrics,
        "conditional_structure_macro_accuracy": conditional_structure_macro_accuracy,
        "conditional_structure_scope": {
            "rows_with_decomposition_ok": decomposition_ok_rows,
        },
        "quantity_constraint_metrics": quantity_constraint_metrics,
        "normalization_metrics": normalization_metrics,
        "claim_metrics": claim_metrics,
    }


def _select_graph_path(row: Dict[str, Any], source: str) -> Optional[Path]:
    asserted = Path(str(row.get("final_kg_path", ""))) if row.get("final_kg_path") else None
    inferred = Path(str(row.get("final_kg_inferred_path", ""))) if row.get("final_kg_inferred_path") else None
    if source == "asserted":
        return asserted
    if source == "inferred":
        return inferred
    if inferred and inferred.exists():
        return inferred
    return asserted


def _parse_graph(path: Path) -> rdflib.Graph:
    graph = rdflib.Graph()
    try:
        graph.parse(path.as_posix(), format="turtle")
        return graph
    except Exception:
        graph = rdflib.Graph()
        graph.parse(path.as_posix(), format="xml")
        return graph


def run_conformance_checks(
    grounding_rows: Sequence[Dict[str, Any]],
    graph_source: str,
) -> Dict[str, Any]:
    """Run SPARQL conformance checks on produced graphs."""
    graph_paths: List[Path] = []
    for row in grounding_rows:
        if row.get("status") != "ok":
            continue
        selected = _select_graph_path(row, source=graph_source)
        if selected and selected.exists():
            graph_paths.append(selected)

    unique_graph_paths: List[Path] = sorted({path.resolve() for path in graph_paths})
    per_graph_results: List[Dict[str, Any]] = []
    per_check_aggregate: Dict[str, Dict[str, Any]] = {
        check.check_id: {
            "check_id": check.check_id,
            "title": check.title,
            "severity": check.severity,
            "graphs_with_violations": 0,
            "total_violations": 0,
        }
        for check in CONFORMANCE_CHECKS
    }

    total_violations = 0
    passed_graphs = 0
    passed_graphs_error_only = 0

    for path in unique_graph_paths:
        graph = _parse_graph(path)
        check_counts: Dict[str, int] = {}
        graph_total = 0
        graph_error_total = 0

        for check in CONFORMANCE_CHECKS:
            rows = list(graph.query(check.query))
            violations = len(rows)
            check_counts[check.check_id] = violations
            graph_total += violations
            if check.severity == "error":
                graph_error_total += violations

            if violations > 0:
                per_check_aggregate[check.check_id]["graphs_with_violations"] += 1
                per_check_aggregate[check.check_id]["total_violations"] += violations

        total_violations += graph_total
        if graph_total == 0:
            passed_graphs += 1
        if graph_error_total == 0:
            passed_graphs_error_only += 1

        per_graph_results.append(
            {
                "graph_path": str(path),
                "total_violations": graph_total,
                "error_violations": graph_error_total,
                "check_violations": check_counts,
            }
        )

    graph_count = len(unique_graph_paths)
    checks_summary = []
    for check in CONFORMANCE_CHECKS:
        aggregate = per_check_aggregate[check.check_id]
        checks_summary.append(
            {
                **aggregate,
                "graphs_with_violations_rate": _round_or_none(
                    _safe_div(aggregate["graphs_with_violations"], graph_count)
                ),
                "mean_violations_per_graph": _round_or_none(
                    _safe_div(aggregate["total_violations"], graph_count)
                ),
            }
        )

    return {
        "graph_source": graph_source,
        "graph_count": graph_count,
        "graphs_passing_all_checks": passed_graphs,
        "graphs_passing_all_checks_rate": _round_or_none(_safe_div(passed_graphs, graph_count)),
        "graphs_passing_error_checks": passed_graphs_error_only,
        "graphs_passing_error_checks_rate": _round_or_none(
            _safe_div(passed_graphs_error_only, graph_count)
        ),
        "total_violations": total_violations,
        "checks": checks_summary,
        "graphs": per_graph_results,
    }


def _read_optional_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    return _load_jsonl(path)


def _status_distribution(rows: Sequence[Dict[str, Any]], key: str = "status") -> Dict[str, int]:
    return dict(Counter(str(row.get(key, "missing")) for row in rows))


def _p95(values: Sequence[float]) -> Optional[float]:
    cleaned = sorted(float(v) for v in values if v is not None and v >= 0)
    if not cleaned:
        return None
    index = max(0, min(len(cleaned) - 1, math.ceil(0.95 * len(cleaned)) - 1))
    return cleaned[index]


def _to_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _build_automatic_summary(
    extraction_rows: Sequence[Dict[str, Any]],
    normalization_rows: Sequence[Dict[str, Any]],
    grounding_rows: Sequence[Dict[str, Any]],
    run_metadata: Dict[str, Any],
) -> Dict[str, Any]:
    extraction_ok = [row for row in extraction_rows if row.get("status") == "ok"]
    normalization_ok = [row for row in normalization_rows if row.get("status") == "ok"]
    grounding_ok = [row for row in grounding_rows if row.get("status") == "ok"]

    extracted_qty = sum(_count_quantity_constraints(row.get("record", {})) for row in extraction_ok)
    normalized_qty = sum(_count_normalized_quantities(row.get("record", {})) for row in normalization_ok)

    extraction_seconds = [_to_float(row.get("extraction_seconds")) for row in extraction_ok]
    normalization_seconds = [_to_float(row.get("normalization_seconds")) for row in normalization_ok]
    grounding_seconds = [_to_float(row.get("grounding_seconds")) for row in grounding_ok]
    inference_seconds = [_to_float(row.get("inference_seconds")) for row in grounding_rows]

    run_duration_seconds = _to_float(run_metadata.get("run_duration_seconds"))
    records_processed = _to_int(run_metadata.get("processed_records"), fallback=len(extraction_rows))
    throughput = _safe_div(records_processed * 60.0, run_duration_seconds) if run_duration_seconds else None

    return {
        "run_id": run_metadata.get("run_id"),
        "dataset_name": run_metadata.get("dataset_name"),
        "records": {
            "extraction": len(extraction_rows),
            "normalization": len(normalization_rows),
            "grounding": len(grounding_rows),
        },
        "status_distributions": {
            "extraction": _status_distribution(extraction_rows),
            "normalization": _status_distribution(normalization_rows),
            "grounding": _status_distribution(grounding_rows),
            "inference": _status_distribution(grounding_rows, key="inference_status"),
        },
        "quantity_coverage_auto": {
            "extracted_quantities": extracted_qty,
            "normalized_quantities": normalized_qty,
            "normalized_over_extracted": _round_or_none(_safe_div(normalized_qty, extracted_qty)),
        },
        "latency": {
            "run_duration_seconds": run_duration_seconds,
            "throughput_records_per_min": _round_or_none(throughput),
            "extraction_p95_seconds": _round_or_none(_p95([value for value in extraction_seconds if value is not None])),
            "normalization_p95_seconds": _round_or_none(
                _p95([value for value in normalization_seconds if value is not None])
            ),
            "grounding_p95_seconds": _round_or_none(_p95([value for value in grounding_seconds if value is not None])),
            "inference_p95_seconds": _round_or_none(_p95([value for value in inference_seconds if value is not None])),
        },
    }


def _format_pct(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.2f}%"


def _report_markdown(report: Dict[str, Any]) -> str:
    automatic = report["automatic_summary"]
    gt = report["ground_truth_metrics"]
    conformance = report["conformance"]
    extraction_metrics = gt["structure_metrics"]
    conditional_extraction_metrics = gt["conditional_structure_metrics"]
    decomposition_metrics = gt["decomposition_metrics"]
    claim_metrics = gt["claim_metrics"]

    lines = [
        "# Evaluation Report",
        "",
        f"- Generated at (UTC): `{report['generated_at_utc']}`",
        f"- Evaluation directory: `{report['evaluation_dir']}`",
        f"- Run ID: `{automatic.get('run_id')}`",
        f"- Dataset: `{automatic.get('dataset_name')}`",
        "",
        "## Automatic Summary",
        "",
        f"- Records (extraction/normalization/grounding): {automatic['records']['extraction']} / {automatic['records']['normalization']} / {automatic['records']['grounding']}",
        f"- Auto quantity coverage (normalized/extracted): {_format_pct(automatic['quantity_coverage_auto']['normalized_over_extracted'])}",
        f"- Run duration (s): {automatic['latency']['run_duration_seconds']}",
        f"- Throughput (records/min): {automatic['latency']['throughput_records_per_min']}",
        "",
        "## Decomposition Metrics (Ground Truth)",
        "",
        f"- Decomposition no-error accuracy: {_format_pct(decomposition_metrics['accuracy'])} ({decomposition_metrics['correct']}/{decomposition_metrics['judged']})",
        f"- Decomposition error rate: {_format_pct(decomposition_metrics['error_rate'])} ({decomposition_metrics['errors']}/{decomposition_metrics['judged']})",
        "",
        "## Structure Metrics (Ground Truth)",
        "",
        f"- Subject accuracy: {_format_pct(extraction_metrics['subject']['accuracy'])} ({extraction_metrics['subject']['correct']}/{extraction_metrics['subject']['judged']})",
        f"- Modality accuracy: {_format_pct(extraction_metrics['modality']['accuracy'])} ({extraction_metrics['modality']['correct']}/{extraction_metrics['modality']['judged']})",
        f"- Condition accuracy: {_format_pct(extraction_metrics['condition']['accuracy'])} ({extraction_metrics['condition']['correct']}/{extraction_metrics['condition']['judged']})",
        f"- Action accuracy: {_format_pct(extraction_metrics['action']['accuracy'])} ({extraction_metrics['action']['correct']}/{extraction_metrics['action']['judged']})",
        f"- Object accuracy: {_format_pct(extraction_metrics['object']['accuracy'])} ({extraction_metrics['object']['correct']}/{extraction_metrics['object']['judged']})",
        f"- Macro structure accuracy: {_format_pct(gt['structure_macro_accuracy'])}",
        "",
        "## Structure Metrics Conditioned on Correct Decomposition",
        "",
        f"- Scope: {gt['conditional_structure_scope']['rows_with_decomposition_ok']} rows with `decomposition_error = false`",
        f"- Subject accuracy: {_format_pct(conditional_extraction_metrics['subject']['accuracy'])} ({conditional_extraction_metrics['subject']['correct']}/{conditional_extraction_metrics['subject']['judged']})",
        f"- Modality accuracy: {_format_pct(conditional_extraction_metrics['modality']['accuracy'])} ({conditional_extraction_metrics['modality']['correct']}/{conditional_extraction_metrics['modality']['judged']})",
        f"- Condition accuracy: {_format_pct(conditional_extraction_metrics['condition']['accuracy'])} ({conditional_extraction_metrics['condition']['correct']}/{conditional_extraction_metrics['condition']['judged']})",
        f"- Action accuracy: {_format_pct(conditional_extraction_metrics['action']['accuracy'])} ({conditional_extraction_metrics['action']['correct']}/{conditional_extraction_metrics['action']['judged']})",
        f"- Object accuracy: {_format_pct(conditional_extraction_metrics['object']['accuracy'])} ({conditional_extraction_metrics['object']['correct']}/{conditional_extraction_metrics['object']['judged']})",
        f"- Conditional macro structure accuracy: {_format_pct(gt['conditional_structure_macro_accuracy'])}",
        "",
        "## Quantity & Normalization Metrics (Ground Truth)",
        "",
        f"- Quantitative constraint precision: {_format_pct(gt['quantity_constraint_metrics']['quantitative_constraint_precision'])}",
        f"- Quantitative constraint recall: {_format_pct(gt['quantity_constraint_metrics']['quantitative_constraint_recall'])}",
        f"- Correct operators / judged: {gt['quantity_constraint_metrics']['operator_accuracy']['correct']}/{gt['quantity_constraint_metrics']['operator_accuracy']['judged']}",
        f"- Correct quantities / judged: {gt['quantity_constraint_metrics']['quantity_value_accuracy']['correct']}/{gt['quantity_constraint_metrics']['quantity_value_accuracy']['judged']}",
        f"- Correct units / judged: {gt['quantity_constraint_metrics']['unit_accuracy']['correct']}/{gt['quantity_constraint_metrics']['unit_accuracy']['judged']}",
        f"- Correct quantity kinds / judged: {gt['quantity_constraint_metrics']['quantity_kind_accuracy']['correct']}/{gt['quantity_constraint_metrics']['quantity_kind_accuracy']['judged']}",
        f"- Correct equivalences / judged: {gt['quantity_constraint_metrics']['equivalence_accuracy']['correct']}/{gt['quantity_constraint_metrics']['equivalence_accuracy']['judged']}",
        "",
        "## KG Claim Metrics (Ground Truth)",
        "",
        f"- TP: {claim_metrics['tp']}",
        f"- FP: {claim_metrics['fp']}",
        f"- FN: {claim_metrics['fn']}",
        f"- Precision: {_format_pct(claim_metrics['precision'])}",
        f"- Recall: {_format_pct(claim_metrics['recall'])}",
        f"- F1: {_format_pct(claim_metrics['f1'])}",
        f"- Claim annotation coverage: {_format_pct(claim_metrics['predicted_claim_annotation_coverage'])}",
        "",
        "## Conformance",
        "",
        f"- Graph source: `{conformance['graph_source']}`",
        f"- Graphs checked: {conformance['graph_count']}",
        f"- Graphs passing all checks: {conformance['graphs_passing_all_checks']} ({_format_pct(conformance['graphs_passing_all_checks_rate'])})",
        f"- Graphs passing error checks: {conformance['graphs_passing_error_checks']} ({_format_pct(conformance['graphs_passing_error_checks_rate'])})",
        f"- Total violations: {conformance['total_violations']}",
        "",
        "### Violations by Check",
        "",
    ]

    for check in conformance["checks"]:
        lines.append(
            f"- {check['check_id']} ({check['severity']}): {check['total_violations']} "
            f"violations across {check['graphs_with_violations']} graphs "
            f"({_format_pct(check['graphs_with_violations_rate'])})"
        )

    return "\n".join(lines) + "\n"


def evaluate_results(
    evaluation_dir: Path,
    extraction_gt_path: Path,
    claims_gt_path: Path,
    conformance_graph_source: str,
) -> Dict[str, Any]:
    extraction_path = evaluation_dir / "extraction.jsonl"
    normalization_path = evaluation_dir / "normalization.jsonl"
    grounding_path = evaluation_dir / "grounding.jsonl"
    run_metadata_path = evaluation_dir / "run_metadata.json"

    extraction_rows = _load_jsonl(extraction_path)
    normalization_rows = _load_jsonl(normalization_path)
    grounding_rows = _load_jsonl(grounding_path)
    run_metadata = (
        json.loads(run_metadata_path.read_text(encoding="utf-8"))
        if run_metadata_path.exists()
        else {}
    )

    extraction_gt_rows = _read_optional_jsonl(extraction_gt_path)
    claims_gt_rows = _read_optional_jsonl(claims_gt_path)

    automatic_summary = _build_automatic_summary(
        extraction_rows=extraction_rows,
        normalization_rows=normalization_rows,
        grounding_rows=grounding_rows,
        run_metadata=run_metadata if isinstance(run_metadata, dict) else {},
    )
    ground_truth_metrics = compute_ground_truth_metrics(
        extraction_gt_rows=extraction_gt_rows,
        claims_gt_rows=claims_gt_rows,
        extraction_rows=extraction_rows,
        normalization_rows=normalization_rows,
    )
    conformance = run_conformance_checks(
        grounding_rows=grounding_rows,
        graph_source=conformance_graph_source,
    )

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "evaluation_dir": str(evaluation_dir),
        "inputs": {
            "extraction_path": str(extraction_path),
            "normalization_path": str(normalization_path),
            "grounding_path": str(grounding_path),
            "run_metadata_path": str(run_metadata_path),
            "ground_truth_extraction_path": str(extraction_gt_path),
            "ground_truth_claims_path": str(claims_gt_path),
        },
        "automatic_summary": automatic_summary,
        "ground_truth_metrics": ground_truth_metrics,
        "conformance": conformance,
    }


def _resolve_path(base: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else (base / path)


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Evaluate pipeline outputs and conformance.")
    parser.add_argument(
        "--evaluation-dir",
        default="src/ontology_req_pipeline/evaluation",
        help="Directory containing extraction/normalization/grounding outputs.",
    )
    parser.add_argument(
        "--ground-truth-extraction",
        default=DEFAULT_EXTRACTION_GT,
        help="Extraction ground-truth JSONL file (relative to evaluation dir if not absolute).",
    )
    parser.add_argument(
        "--ground-truth-claims",
        default=DEFAULT_CLAIMS_GT,
        help="Claims ground-truth JSONL file (relative to evaluation dir if not absolute).",
    )
    parser.add_argument(
        "--output-json",
        default=DEFAULT_REPORT_JSON,
        help="JSON report output path (relative to evaluation dir if not absolute).",
    )
    parser.add_argument(
        "--output-md",
        default=DEFAULT_REPORT_MD,
        help="Markdown report output path (relative to evaluation dir if not absolute).",
    )
    parser.add_argument(
        "--init-ground-truth",
        action="store_true",
        help="Generate ground-truth templates from current outputs.",
    )
    parser.add_argument(
        "--overwrite-ground-truth",
        action="store_true",
        help="Allow overwriting existing ground-truth files when --init-ground-truth is used.",
    )
    parser.add_argument(
        "--claims-graph-source",
        choices=("asserted", "inferred", "prefer_inferred"),
        default="asserted",
        help="Graph source used to seed claim-level ground-truth templates.",
    )
    parser.add_argument(
        "--conformance-graph-source",
        choices=("asserted", "inferred", "prefer_inferred"),
        default="prefer_inferred",
        help="Graph source used by SPARQL conformance checks.",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)
    evaluation_dir = Path(args.evaluation_dir).resolve()
    if not evaluation_dir.exists():
        raise SystemExit(f"Evaluation directory does not exist: {evaluation_dir}")

    extraction_path = evaluation_dir / "extraction.jsonl"
    normalization_path = evaluation_dir / "normalization.jsonl"
    grounding_path = evaluation_dir / "grounding.jsonl"
    for path in (extraction_path, normalization_path, grounding_path):
        if not path.exists():
            raise SystemExit(f"Missing required input file: {path}")

    extraction_gt_path = _resolve_path(evaluation_dir, args.ground_truth_extraction)
    claims_gt_path = _resolve_path(evaluation_dir, args.ground_truth_claims)
    output_json_path = _resolve_path(evaluation_dir, args.output_json)
    output_md_path = _resolve_path(evaluation_dir, args.output_md)

    extraction_rows = _load_jsonl(extraction_path)
    normalization_rows = _load_jsonl(normalization_path)
    grounding_rows = _load_jsonl(grounding_path)

    if args.init_ground_truth:
        extraction_template = build_extraction_ground_truth_template(
            extraction_rows=extraction_rows,
            normalization_rows=normalization_rows,
        )
        claims_template = build_claim_ground_truth_template(
            extraction_rows=extraction_rows,
            grounding_rows=grounding_rows,
            claims_graph_source=args.claims_graph_source,
        )

        if extraction_gt_path.exists() and not args.overwrite_ground_truth:
            raise SystemExit(
                f"Ground truth file already exists: {extraction_gt_path}. "
                "Use --overwrite-ground-truth to replace it."
            )
        if claims_gt_path.exists() and not args.overwrite_ground_truth:
            raise SystemExit(
                f"Ground truth file already exists: {claims_gt_path}. "
                "Use --overwrite-ground-truth to replace it."
            )

        extraction_gt_path.parent.mkdir(parents=True, exist_ok=True)
        claims_gt_path.parent.mkdir(parents=True, exist_ok=True)
        _write_jsonl(extraction_gt_path, extraction_template)
        _write_jsonl(claims_gt_path, claims_template)
        print(f"Wrote extraction ground-truth template: {extraction_gt_path}")
        print(f"Wrote claims ground-truth template:     {claims_gt_path}")

    report = evaluate_results(
        evaluation_dir=evaluation_dir,
        extraction_gt_path=extraction_gt_path,
        claims_gt_path=claims_gt_path,
        conformance_graph_source=args.conformance_graph_source,
    )

    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    output_md_path.parent.mkdir(parents=True, exist_ok=True)
    output_json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    output_md_path.write_text(_report_markdown(report), encoding="utf-8")

    print(f"Wrote JSON report: {output_json_path}")
    print(f"Wrote Markdown report: {output_md_path}")


if __name__ == "__main__":
    main()
