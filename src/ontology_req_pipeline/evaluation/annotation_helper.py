"""Utilities to make JSONL ground-truth annotation easier.

Examples:
  python -m ontology_req_pipeline.evaluation.annotation_helper split \
    --input src/ontology_req_pipeline/evaluation/ground_truth_extraction.jsonl \
    --output-dir src/ontology_req_pipeline/evaluation/ground_truth_extraction_items

  python -m ontology_req_pipeline.evaluation.annotation_helper merge \
    --input-dir src/ontology_req_pipeline/evaluation/ground_truth_extraction_items \
    --output src/ontology_req_pipeline/evaluation/ground_truth_extraction.jsonl
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence


def _looks_like_extraction_annotation_row(row: Dict[str, Any]) -> bool:
    return (
        isinstance(row, dict)
        and "prediction" in row
        and "labels" in row
        and "quantity_constraints" in row
        and "missing_quantitative_constraints" in row
    )


def _ensure_annotation_defaults(row: Dict[str, Any]) -> Dict[str, Any]:
    if _looks_like_extraction_annotation_row(row):
        labels = row.get("labels")
        if isinstance(labels, dict):
            labels.setdefault("decomposition_error", None)
    return row


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
            rows.append(_ensure_annotation_defaults(payload))
    return rows


def _write_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8") as outfile:
        for row in rows:
            outfile.write(json.dumps(_ensure_annotation_defaults(row), ensure_ascii=False) + "\n")


def _to_int(value: Any, fallback: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _filename_for_row(row: Dict[str, Any], index: int) -> str:
    idx = _to_int(row.get("idx"), fallback=-1)
    req_idx = _to_int(row.get("req_idx"), fallback=-1)
    if idx >= 0 and req_idx >= 0:
        return f"idx_{idx:05d}_req_{req_idx:03d}.json"
    if idx >= 0:
        return f"idx_{idx:05d}_{index:05d}.json"
    return f"row_{index:05d}.json"


def split_jsonl(input_path: Path, output_dir: Path) -> None:
    rows = _load_jsonl(input_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest: List[Dict[str, Any]] = []
    for i, row in enumerate(rows):
        filename = _filename_for_row(row, index=i)
        file_path = output_dir / filename
        file_path.write_text(json.dumps(row, indent=2, ensure_ascii=False), encoding="utf-8")
        manifest.append(
            {
                "filename": filename,
                "idx": row.get("idx"),
                "req_idx": row.get("req_idx"),
            }
        )

    (output_dir / "_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )


def merge_json_dir(input_dir: Path, output_path: Path) -> None:
    if not input_dir.exists():
        raise FileNotFoundError(f"Input dir not found: {input_dir}")

    files = sorted(path for path in input_dir.glob("*.json") if path.name != "_manifest.json")
    rows: List[Dict[str, Any]] = []
    for file_path in files:
        payload = json.loads(file_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Expected object in {file_path}")
        rows.append(_ensure_annotation_defaults(payload))

    rows.sort(key=lambda row: (_to_int(row.get("idx")), _to_int(row.get("req_idx"))))
    _write_jsonl(output_path, rows)


def export_pretty(input_path: Path, output_path: Path) -> None:
    rows = _load_jsonl(input_path)
    output_path.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")


def import_pretty(input_path: Path, output_path: Path) -> None:
    payload = json.loads(input_path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Expected JSON array in {input_path}")
    rows: List[Dict[str, Any]] = []
    for i, row in enumerate(payload):
        if not isinstance(row, dict):
            raise ValueError(f"Expected object at index {i} in {input_path}")
        rows.append(_ensure_annotation_defaults(row))
    rows.sort(key=lambda row: (_to_int(row.get("idx")), _to_int(row.get("req_idx"))))
    _write_jsonl(output_path, rows)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ground-truth annotation helper.")
    sub = parser.add_subparsers(dest="command", required=True)

    p_split = sub.add_parser("split", help="Split JSONL into many pretty JSON files.")
    p_split.add_argument("--input", required=True, help="Input JSONL path.")
    p_split.add_argument("--output-dir", required=True, help="Output folder path.")

    p_merge = sub.add_parser("merge", help="Merge per-record JSON files back to JSONL.")
    p_merge.add_argument("--input-dir", required=True, help="Folder containing .json files.")
    p_merge.add_argument("--output", required=True, help="Output JSONL path.")

    p_export = sub.add_parser("export-pretty", help="Convert JSONL to one pretty JSON array file.")
    p_export.add_argument("--input", required=True, help="Input JSONL path.")
    p_export.add_argument("--output", required=True, help="Output JSON path.")

    p_import = sub.add_parser("import-pretty", help="Convert pretty JSON array back to JSONL.")
    p_import.add_argument("--input", required=True, help="Input JSON path.")
    p_import.add_argument("--output", required=True, help="Output JSONL path.")

    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    command = args.command

    if command == "split":
        split_jsonl(Path(args.input), Path(args.output_dir))
        print(f"Split completed: {args.output_dir}")
        return

    if command == "merge":
        merge_json_dir(Path(args.input_dir), Path(args.output))
        print(f"Merged JSONL written: {args.output}")
        return

    if command == "export-pretty":
        export_pretty(Path(args.input), Path(args.output))
        print(f"Pretty JSON written: {args.output}")
        return

    if command == "import-pretty":
        import_pretty(Path(args.input), Path(args.output))
        print(f"JSONL written: {args.output}")
        return

    raise RuntimeError(f"Unknown command: {command}")


if __name__ == "__main__":
    main()
