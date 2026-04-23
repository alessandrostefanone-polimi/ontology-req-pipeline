"""Inter-annotator agreement for split extraction annotation folders.

This script evaluates agreement directly on per-item JSON annotation folders,
before they are merged back into JSONL files.

Usage:
    python -m ontology_req_pipeline.evaluation.iaa \
      --annotation-parent-dir src/ontology_req_pipeline/evaluation/fsae_test_pipeline_rerun

It auto-discovers folders named ``ground_truth_extraction_items*`` under the
given parent directory. You can also specify them explicitly:

    python -m ontology_req_pipeline.evaluation.iaa \
      --annotation-parent-dir src/ontology_req_pipeline/evaluation/fsae_test_pipeline_rerun \
      --annotation-dir ground_truth_extraction_items \
      --annotation-dir ground_truth_extraction_items_ann1 \
      --annotation-dir ground_truth_extraction_items_ann2
"""

from __future__ import annotations

import argparse
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


REQUIREMENT_LEVEL_LABELS: Tuple[str, ...] = (
    "decomposition_error",
    "subject_correct",
    "modality_correct",
    "condition_correct",
    "action_correct",
    "object_correct",
)

QUANTITY_LEVEL_LABELS: Tuple[str, ...] = (
    "is_true_quantitative_constraint",
    "operator_correct",
    "quantity_value_correct",
    "unit_correct",
    "quantity_kind_correct",
    "equivalence_correct",
)

DEFAULT_REPORT_JSON = "iaa_report.json"
DEFAULT_REPORT_MD = "iaa_report.md"

RequirementKey = Tuple[int, int]
QuantityKey = Tuple[int, int, int]


@dataclass(frozen=True)
class InvalidFile:
    annotator: str
    path: str
    error: str


def _round_or_none(value: Optional[float], digits: int = 6) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), digits)


def _safe_div(numerator: float, denominator: float) -> Optional[float]:
    if denominator == 0:
        return None
    return numerator / denominator


def _n_choose_2(value: int) -> int:
    if value < 2:
        return 0
    return (value * (value - 1)) // 2


def _to_int(value: Any, fallback: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _as_optional_bool(value: Any) -> Optional[bool]:
    if value is True:
        return True
    if value is False:
        return False
    return None


def _discover_annotation_dirs(parent_dir: Path) -> List[Path]:
    candidates = sorted(
        path
        for path in parent_dir.iterdir()
        if path.is_dir() and path.name.startswith("ground_truth_extraction_items")
    )
    return candidates


def _load_annotation_dir(
    path: Path,
) -> Tuple[
    Dict[RequirementKey, Dict[str, Optional[bool]]],
    Dict[QuantityKey, Dict[str, Optional[bool]]],
    List[InvalidFile],
]:
    requirement_labels: Dict[RequirementKey, Dict[str, Optional[bool]]] = {}
    quantity_labels: Dict[QuantityKey, Dict[str, Optional[bool]]] = {}
    invalid_files: List[InvalidFile] = []

    for file_path in sorted(path.glob("*.json")):
        if file_path.name == "_manifest.json":
            continue

        try:
            payload = json.loads(file_path.read_text(encoding="utf-8"))
        except Exception as exc:
            invalid_files.append(
                InvalidFile(
                    annotator=path.name,
                    path=str(file_path),
                    error=f"{type(exc).__name__}: {exc}",
                )
            )
            continue

        if not isinstance(payload, dict):
            invalid_files.append(
                InvalidFile(
                    annotator=path.name,
                    path=str(file_path),
                    error="ValueError: expected top-level JSON object",
                )
            )
            continue

        req_key = (_to_int(payload.get("idx")), _to_int(payload.get("req_idx")))
        root = payload.get("labels", {})
        requirement_labels[req_key] = {
            label: _as_optional_bool(root.get(label)) for label in REQUIREMENT_LEVEL_LABELS
        }

        for constraint in payload.get("quantity_constraints", []):
            if not isinstance(constraint, dict):
                continue
            constraint_key = (
                req_key[0],
                req_key[1],
                _to_int(constraint.get("constraint_idx")),
            )
            qlabels = constraint.get("labels", {})
            quantity_labels[constraint_key] = {
                label: _as_optional_bool(qlabels.get(label)) for label in QUANTITY_LEVEL_LABELS
            }

    return requirement_labels, quantity_labels, invalid_files


def _build_label_ratings(
    records_by_annotator: Mapping[str, Mapping[Any, Mapping[str, Optional[bool]]]],
    label: str,
) -> Dict[Any, Dict[str, bool]]:
    ratings: Dict[Any, Dict[str, bool]] = {}
    for annotator, records in records_by_annotator.items():
        for item_key, labels in records.items():
            value = labels.get(label)
            if value is None:
                continue
            ratings.setdefault(item_key, {})[annotator] = value
    return ratings


def _pairwise_agreement_stats(
    ratings_by_item: Mapping[Any, Mapping[str, bool]],
) -> Dict[str, Any]:
    total_pairs = 0
    agree_pairs = 0
    items_with_2plus_raters = 0
    max_raters_on_item = 0

    for annotator_values in ratings_by_item.values():
        values = list(annotator_values.values())
        n = len(values)
        max_raters_on_item = max(max_raters_on_item, n)
        if n < 2:
            continue
        items_with_2plus_raters += 1
        pair_count = _n_choose_2(n)
        total_pairs += pair_count
        true_count = sum(value is True for value in values)
        false_count = n - true_count
        agree_pairs += _n_choose_2(true_count) + _n_choose_2(false_count)

    return {
        "rated_items": len(ratings_by_item),
        "items_with_2plus_raters": items_with_2plus_raters,
        "max_raters_on_single_item": max_raters_on_item,
        "agreeing_pairs": agree_pairs,
        "total_pairs": total_pairs,
        "agreement": _round_or_none(_safe_div(agree_pairs, total_pairs)),
    }


def _krippendorff_alpha_nominal(
    ratings_by_item: Mapping[Any, Mapping[str, bool]]
) -> Optional[float]:
    categories = (False, True)
    coincidence = {
        cat_i: {cat_j: 0.0 for cat_j in categories} for cat_i in categories
    }
    total_pairable_ratings = 0.0

    for annotator_values in ratings_by_item.values():
        values = list(annotator_values.values())
        n_i = len(values)
        if n_i < 2:
            continue
        counts = Counter(values)
        for cat_i in categories:
            count_i = counts.get(cat_i, 0)
            if count_i == 0:
                continue
            for cat_j in categories:
                count_j = counts.get(cat_j, 0)
                if count_j == 0:
                    continue
                if cat_i == cat_j:
                    coincidence[cat_i][cat_j] += count_i * (count_i - 1) / (n_i - 1)
                else:
                    coincidence[cat_i][cat_j] += count_i * count_j / (n_i - 1)
        total_pairable_ratings += n_i

    if total_pairable_ratings <= 1:
        return None

    marginals = {
        category: sum(coincidence[category].values()) for category in categories
    }
    observed_disagreement = (
        coincidence[False][True] + coincidence[True][False]
    ) / total_pairable_ratings
    expected_num = (
        marginals[False] * marginals[True] + marginals[True] * marginals[False]
    )
    expected_den = total_pairable_ratings * (total_pairable_ratings - 1)
    expected_disagreement = _safe_div(expected_num, expected_den)
    if expected_disagreement is None or expected_disagreement == 0:
        return None

    alpha = 1.0 - (observed_disagreement / expected_disagreement)
    return _round_or_none(alpha)


def _cohen_kappa(values_a: Sequence[bool], values_b: Sequence[bool]) -> Optional[float]:
    if len(values_a) != len(values_b) or not values_a:
        return None

    n = len(values_a)
    agree = sum(a == b for a, b in zip(values_a, values_b))
    p_observed = agree / n

    p_true_a = sum(values_a) / n
    p_true_b = sum(values_b) / n
    p_false_a = 1.0 - p_true_a
    p_false_b = 1.0 - p_true_b
    p_expected = (p_true_a * p_true_b) + (p_false_a * p_false_b)
    if p_expected == 1.0:
        return None

    return _round_or_none((p_observed - p_expected) / (1.0 - p_expected))


def _pairwise_cohen_summary(
    ratings_by_item: Mapping[Any, Mapping[str, bool]],
    annotators: Sequence[str],
) -> Dict[str, Any]:
    pair_rows: List[Dict[str, Any]] = []
    weighted_kappa_sum = 0.0
    weighted_overlap_sum = 0

    for index, annotator_a in enumerate(annotators):
        for annotator_b in annotators[index + 1 :]:
            values_a: List[bool] = []
            values_b: List[bool] = []
            for annotator_values in ratings_by_item.values():
                if annotator_a not in annotator_values or annotator_b not in annotator_values:
                    continue
                values_a.append(annotator_values[annotator_a])
                values_b.append(annotator_values[annotator_b])

            overlap = len(values_a)
            if overlap == 0:
                continue

            agreement = sum(a == b for a, b in zip(values_a, values_b))
            kappa = _cohen_kappa(values_a, values_b)
            pair_rows.append(
                {
                    "annotator_a": annotator_a,
                    "annotator_b": annotator_b,
                    "overlap": overlap,
                    "agreement": _round_or_none(agreement / overlap),
                    "kappa": kappa,
                }
            )
            if kappa is not None:
                weighted_kappa_sum += kappa * overlap
                weighted_overlap_sum += overlap

    kappas = [row["kappa"] for row in pair_rows if row["kappa"] is not None]
    return {
        "weighted_mean_kappa": _round_or_none(
            _safe_div(weighted_kappa_sum, weighted_overlap_sum)
        ),
        "min_kappa": _round_or_none(min(kappas)) if kappas else None,
        "max_kappa": _round_or_none(max(kappas)) if kappas else None,
        "pairs_compared": len(pair_rows),
        "pair_details": pair_rows,
    }


def _annotator_rating_counts(
    records_by_annotator: Mapping[str, Mapping[Any, Mapping[str, Optional[bool]]]],
    label: str,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for annotator, records in records_by_annotator.items():
        values = [labels.get(label) for labels in records.values()]
        true_count = sum(value is True for value in values)
        false_count = sum(value is False for value in values)
        rated = true_count + false_count
        rows.append(
            {
                "annotator": annotator,
                "rated": rated,
                "true": true_count,
                "false": false_count,
            }
        )
    return rows


def _summarize_label(
    records_by_annotator: Mapping[str, Mapping[Any, Mapping[str, Optional[bool]]]],
    label: str,
    annotators: Sequence[str],
) -> Dict[str, Any]:
    ratings_by_item = _build_label_ratings(records_by_annotator, label)
    value_counter = Counter()
    for annotator_values in ratings_by_item.values():
        value_counter.update(annotator_values.values())

    pairwise_agreement = _pairwise_agreement_stats(ratings_by_item)
    pairwise_cohen = _pairwise_cohen_summary(ratings_by_item, annotators)

    return {
        "label": label,
        "individual_ratings": {
            "total": int(value_counter[True] + value_counter[False]),
            "true": int(value_counter[True]),
            "false": int(value_counter[False]),
        },
        "pooled_pairwise_agreement": pairwise_agreement,
        "krippendorff_alpha_nominal": _krippendorff_alpha_nominal(ratings_by_item),
        "pairwise_cohen_kappa": pairwise_cohen,
        "annotator_rating_counts": _annotator_rating_counts(records_by_annotator, label),
    }


def _mean_or_none(values: Sequence[Optional[float]]) -> Optional[float]:
    present = [value for value in values if value is not None]
    if not present:
        return None
    return _round_or_none(sum(present) / len(present))


def _build_group_aggregate(label_summaries: Mapping[str, Dict[str, Any]]) -> Dict[str, Any]:
    summaries = list(label_summaries.values())
    return {
        "label_count": len(summaries),
        "total_individual_ratings": sum(
            summary["individual_ratings"]["total"] for summary in summaries
        ),
        "total_items_with_2plus_raters": sum(
            summary["pooled_pairwise_agreement"]["items_with_2plus_raters"]
            for summary in summaries
        ),
        "macro_pairwise_agreement": _mean_or_none(
            [summary["pooled_pairwise_agreement"]["agreement"] for summary in summaries]
        ),
        "macro_krippendorff_alpha_nominal": _mean_or_none(
            [summary["krippendorff_alpha_nominal"] for summary in summaries]
        ),
        "macro_weighted_mean_cohen_kappa": _mean_or_none(
            [summary["pairwise_cohen_kappa"]["weighted_mean_kappa"] for summary in summaries]
        ),
    }


def _summarize_group(
    records_by_annotator: Mapping[str, Mapping[Any, Mapping[str, Optional[bool]]]],
    labels: Sequence[str],
    annotators: Sequence[str],
) -> Dict[str, Any]:
    label_summaries = {
        label: _summarize_label(records_by_annotator, label, annotators) for label in labels
    }
    return {
        "aggregate": _build_group_aggregate(label_summaries),
        "labels": label_summaries,
    }


def _format_pct(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{value * 100:.1f}%"


def _render_label_table(group_title: str, labels: Mapping[str, Dict[str, Any]]) -> List[str]:
    lines = [f"## {group_title}", ""]
    lines.append("| Label | Ratings | Items >=2 raters | Pairwise agreement | Alpha | Weighted mean kappa |")
    lines.append("| --- | ---: | ---: | ---: | ---: | ---: |")

    for label_name, summary in labels.items():
        pairwise = summary["pooled_pairwise_agreement"]
        cohen = summary["pairwise_cohen_kappa"]
        lines.append(
            "| "
            + f"`{label_name}`"
            + " | "
            + f"{summary['individual_ratings']['total']}"
            + " | "
            + f"{pairwise['items_with_2plus_raters']}"
            + " | "
            + f"{_format_pct(pairwise['agreement'])}"
            + " | "
            + f"{summary['krippendorff_alpha_nominal'] if summary['krippendorff_alpha_nominal'] is not None else 'n/a'}"
            + " | "
            + f"{cohen['weighted_mean_kappa'] if cohen['weighted_mean_kappa'] is not None else 'n/a'}"
            + " |"
        )

    lines.append("")
    return lines


def _render_group_aggregate(group_title: str, aggregate: Mapping[str, Any]) -> List[str]:
    return [
        f"## {group_title} Aggregate",
        "",
        "| Labels | Total ratings | Total items >=2 raters | Macro pairwise agreement | Macro alpha | Macro weighted mean kappa |",
        "| ---: | ---: | ---: | ---: | ---: | ---: |",
        "| "
        + f"{aggregate['label_count']}"
        + " | "
        + f"{aggregate['total_individual_ratings']}"
        + " | "
        + f"{aggregate['total_items_with_2plus_raters']}"
        + " | "
        + f"{_format_pct(aggregate['macro_pairwise_agreement'])}"
        + " | "
        + (
            f"{aggregate['macro_krippendorff_alpha_nominal']}"
            if aggregate["macro_krippendorff_alpha_nominal"] is not None
            else "n/a"
        )
        + " | "
        + (
            f"{aggregate['macro_weighted_mean_cohen_kappa']}"
            if aggregate["macro_weighted_mean_cohen_kappa"] is not None
            else "n/a"
        )
        + " |",
        "",
    ]


def _render_pair_details(
    group_title: str,
    labels: Mapping[str, Dict[str, Any]],
) -> List[str]:
    lines = [f"## {group_title} Pairwise Details", ""]

    for label_name, summary in labels.items():
        lines.append(f"### `{label_name}`")
        pair_rows = summary["pairwise_cohen_kappa"]["pair_details"]
        if not pair_rows:
            lines.append("No pairwise overlap with non-missing labels.")
            lines.append("")
            continue

        lines.append("| Pair | Overlap | Agreement | Kappa |")
        lines.append("| --- | ---: | ---: | ---: |")
        for row in pair_rows:
            pair_name = f"{row['annotator_a']} vs {row['annotator_b']}"
            lines.append(
                "| "
                + pair_name
                + " | "
                + f"{row['overlap']}"
                + " | "
                + f"{_format_pct(row['agreement'])}"
                + " | "
                + f"{row['kappa'] if row['kappa'] is not None else 'n/a'}"
                + " |"
            )
        lines.append("")

    return lines


def _build_markdown_report(report: Dict[str, Any]) -> str:
    lines: List[str] = [
        "# Inter-Annotator Agreement Report",
        "",
        f"- Generated at: `{report['generated_at']}`",
        f"- Annotation parent dir: `{report['annotation_parent_dir']}`",
        f"- Annotators included: {', '.join(f'`{name}`' for name in report['annotators'])}",
        "",
    ]

    invalid_files = report["invalid_files"]
    if invalid_files:
        lines.append("## Invalid Files Skipped")
        lines.append("")
        for entry in invalid_files:
            lines.append(
                f"- `{entry['annotator']}`: `{entry['path']}` ({entry['error']})"
            )
        lines.append("")

    lines.extend(
        _render_group_aggregate(
            "Requirement-Level Labels",
            report["requirement_level"]["aggregate"],
        )
    )
    lines.extend(
        _render_label_table(
            "Requirement-Level Labels",
            report["requirement_level"]["labels"],
        )
    )
    lines.extend(
        _render_group_aggregate(
            "Quantity-Constraint Labels",
            report["quantity_level"]["aggregate"],
        )
    )
    lines.extend(
        _render_label_table(
            "Quantity-Constraint Labels",
            report["quantity_level"]["labels"],
        )
    )
    lines.extend(
        _render_pair_details(
            "Requirement-Level Labels",
            report["requirement_level"]["labels"],
        )
    )
    lines.extend(
        _render_pair_details(
            "Quantity-Constraint Labels",
            report["quantity_level"]["labels"],
        )
    )
    return "\n".join(lines).strip() + "\n"


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute inter-annotator agreement on split extraction annotation folders."
    )
    parser.add_argument(
        "--annotation-parent-dir",
        required=True,
        help="Directory containing ground_truth_extraction_items* folders.",
    )
    parser.add_argument(
        "--annotation-dir",
        action="append",
        default=[],
        help="Specific annotation folder name under --annotation-parent-dir. Repeatable.",
    )
    parser.add_argument(
        "--report-json",
        default=DEFAULT_REPORT_JSON,
        help="Output JSON report filename or absolute path.",
    )
    parser.add_argument(
        "--report-md",
        default=DEFAULT_REPORT_MD,
        help="Output Markdown report filename or absolute path.",
    )
    return parser.parse_args(argv)


def _resolve_output_path(parent_dir: Path, value: str) -> Path:
    candidate = Path(value)
    if candidate.is_absolute():
        return candidate
    return parent_dir / candidate


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = _parse_args(argv)
    parent_dir = Path(args.annotation_parent_dir)
    if not parent_dir.exists():
        raise FileNotFoundError(f"Annotation parent dir not found: {parent_dir}")

    if args.annotation_dir:
        annotation_dirs = [parent_dir / name for name in args.annotation_dir]
    else:
        annotation_dirs = _discover_annotation_dirs(parent_dir)

    if not annotation_dirs:
        raise FileNotFoundError(
            f"No annotation folders found under {parent_dir} matching ground_truth_extraction_items*"
        )

    for directory in annotation_dirs:
        if not directory.exists():
            raise FileNotFoundError(f"Annotation folder not found: {directory}")

    requirement_records_by_annotator: Dict[
        str, Dict[RequirementKey, Dict[str, Optional[bool]]]
    ] = {}
    quantity_records_by_annotator: Dict[
        str, Dict[QuantityKey, Dict[str, Optional[bool]]]
    ] = {}
    invalid_files: List[InvalidFile] = []

    for directory in annotation_dirs:
        annotator = directory.name
        requirement_records, quantity_records, issues = _load_annotation_dir(directory)
        requirement_records_by_annotator[annotator] = requirement_records
        quantity_records_by_annotator[annotator] = quantity_records
        invalid_files.extend(issues)

    annotators = sorted(requirement_records_by_annotator)
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "annotation_parent_dir": str(parent_dir),
        "annotators": annotators,
        "invalid_files": [
            {"annotator": item.annotator, "path": item.path, "error": item.error}
            for item in invalid_files
        ],
        "requirement_level": _summarize_group(
            requirement_records_by_annotator,
            REQUIREMENT_LEVEL_LABELS,
            annotators,
        ),
        "quantity_level": _summarize_group(
            quantity_records_by_annotator,
            QUANTITY_LEVEL_LABELS,
            annotators,
        ),
    }

    report_json_path = _resolve_output_path(parent_dir, args.report_json)
    report_md_path = _resolve_output_path(parent_dir, args.report_md)
    report_json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    report_md_path.write_text(_build_markdown_report(report), encoding="utf-8")

    print(f"IAA JSON report written: {report_json_path}")
    print(f"IAA Markdown report written: {report_md_path}")


if __name__ == "__main__":
    main()
