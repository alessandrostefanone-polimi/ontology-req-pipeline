"""Configuration utilities for the ontology requirements pipeline."""

from dataclasses import dataclass
from pathlib import Path


@dataclass
class PipelineConfig:
    """Basic pipeline configuration placeholder."""

    shapes_path: Path = Path(__file__).parent / "validation" / "shapes"
    example_shapes_file: str = "flow_rate_example_shapes.ttl"


# TODO: Extend with logging configuration, model paths, and IOF alignment settings.
