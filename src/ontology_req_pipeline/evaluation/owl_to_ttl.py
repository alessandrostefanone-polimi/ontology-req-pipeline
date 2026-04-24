"""Convert an OWL file to Turtle (.ttl).

Usage:
    python -m ontology_req_pipeline.evaluation.owl_to_ttl --input path/to/input.owl
    python -m ontology_req_pipeline.evaluation.owl_to_ttl --input path/to/input.owl --output path/to/output.ttl
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Optional

from rdflib import Graph


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert an OWL graph file to Turtle (.ttl).")
    parser.add_argument(
        "--input",
        required=True,
        help="Path to OWL graph (e.g., .owl, .rdf, .xml, .ttl).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Output Turtle path. Defaults to input path with .ttl extension.",
    )
    parser.add_argument(
        "--input-format",
        default=None,
        help=(
            "Optional rdflib parser format (e.g., xml, turtle, n3, nt, trig, json-ld). "
            "If omitted, rdflib will auto-detect."
        ),
    )
    return parser.parse_args()


def convert_owl_to_ttl(input_path: Path, output_path: Path, input_format: Optional[str] = None) -> None:
    graph = Graph()
    graph.parse(input_path.as_posix(), format=input_format)
    graph.serialize(destination=output_path.as_posix(), format="turtle")


def main() -> None:
    args = _parse_args()
    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    output_path = (
        Path(args.output).expanduser().resolve()
        if args.output
        else input_path.with_suffix(".ttl")
    )

    convert_owl_to_ttl(input_path=input_path, output_path=output_path, input_format=args.input_format)
    print(f"Saved Turtle graph to: {output_path}")


if __name__ == "__main__":
    main()
