"""Example script running the pipeline end-to-end."""

from __future__ import annotations

import json
from pathlib import Path

import rdflib

from ontology_req_pipeline.extraction.llm_extractor import get_default_extractor
from ontology_req_pipeline.ontology.template_instantiation import requirement_to_rdf
from ontology_req_pipeline.validation.shacl_runner import validate_graph
from ontology_req_pipeline.config import PipelineConfig


def main() -> None:
    """Execute the example pipeline."""
    example_path = Path(__file__).parent / "example_input.json"
    with example_path.open("r", encoding="utf-8") as f:
        example_json = json.load(f)

    extractor = get_default_extractor()
    extraction = extractor.extract(example_json["original_text"])

    graph = requirement_to_rdf(extraction)
    shapes_path = PipelineConfig().shapes_path / PipelineConfig().example_shapes_file
    shapes_graph = rdflib.Graph().parse(shapes_path)

    conforms, report = validate_graph(graph, shapes_graph)

    print("=== Extracted JSON ===")
    print(extraction.pretty_print())
    print("\n=== RDF (Turtle) ===")
    print(graph.serialize(format="turtle"))
    print("\n=== SHACL Validation ===")
    print(f"Conforms: {conforms}")
    print(report)


if __name__ == "__main__":
    main()
