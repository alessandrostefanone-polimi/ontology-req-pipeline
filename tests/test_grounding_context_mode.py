from __future__ import annotations

from pathlib import Path

import pytest

import ontology_req_pipeline.ontology.agentic_kg_builder as builder_module
from ontology_req_pipeline.ontology.agentic_kg_builder import AgenticKGBuilder


def _context_builder(mode: str) -> AgenticKGBuilder:
    builder = AgenticKGBuilder.__new__(AgenticKGBuilder)
    builder.CORE_PATH = Path(__file__).resolve().parents[1] / "ontologies" / "Core.rdf"
    builder.CORE_CONTEXT = builder.CORE_PATH.read_text(encoding="utf-8", errors="ignore")
    builder.iof_ns = "https://spec.industrialontologies.org/ontology/core/Core/"
    builder.ontology_context_mode = mode
    builder._compact_context_cache = {}
    return builder


def test_compact_context_is_small_and_preserves_required_grounding_terms() -> None:
    builder = _context_builder("compact")
    payload = {
        "idx": 0,
        "original_text": "The valve shall control the flow of liquids and gases.",
        "requirements": [],
    }

    context = builder._ontology_context_for_payload(payload)

    assert len(context) < 10_000
    assert len(context) < len(builder.CORE_CONTEXT) / 20
    assert "Mode: compact IOF Core signature" in context
    assert "iof:RequirementSpecification [class]" in context
    assert "iof:requirementSatisfiedBy [object property]" in context
    assert "iof:prescribes [object property]" in context
    assert "<rdf:RDF" not in context


def test_full_context_returns_complete_core_ontology() -> None:
    builder = _context_builder("full")

    context = builder._ontology_context_for_payload({"idx": 0})

    assert context == builder.CORE_CONTEXT
    assert "<rdf:RDF" in context


def test_constructor_initializes_namespaces_before_compact_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeOntology:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def add_axiom(self, _axiom) -> None:
            pass

    monkeypatch.setattr(builder_module, "OpenAI", lambda: object())
    monkeypatch.setattr(builder_module, "SyncOntology", FakeOntology)
    monkeypatch.setattr(AgenticKGBuilder, "_load_tbox_axioms", lambda _self: [])
    monkeypatch.setattr(
        AgenticKGBuilder,
        "_load_inverse_object_property_pairs",
        lambda _self: [],
    )

    builder = AgenticKGBuilder(
        tbox_path=Path(__file__).resolve().parents[1] / "ontologies" / "Core.rdf",
        record={
            "idx": 0,
            "original_text": "The valve shall control liquid flow.",
            "requirements": [],
        },
        llm_provider="openai",
        ontology_context_mode="compact",
    )

    assert builder.iof_ns.endswith("/Core/")
    assert "Mode: compact IOF Core signature" in builder.ontology_context


@pytest.mark.parametrize(
    ("requested", "expected"),
    [
        ("compact", "compact"),
        ("token-efficient", "compact"),
        ("full", "full"),
        ("whole-ontology", "full"),
    ],
)
def test_context_mode_aliases(requested: str, expected: str) -> None:
    assert AgenticKGBuilder._normalize_ontology_context_mode(requested) == expected


def test_invalid_context_mode_is_rejected() -> None:
    with pytest.raises(ValueError, match="ontology_context_mode"):
        AgenticKGBuilder._normalize_ontology_context_mode("summary-ish")
