from __future__ import annotations

from rdflib import Graph

import ontology_req_pipeline.ontology.agentic_kg_builder as builder_module
from ontology_req_pipeline.cli import _grounding_failure_diagnostics
from ontology_req_pipeline.ontology.agentic_kg_builder import AgenticKGBuilder
from owlapy.owl_axiom import (
    OWLClassAssertionAxiom,
    OWLDataPropertyAssertionAxiom,
    OWLObjectPropertyAssertionAxiom,
)


class FakeOntology:
    created: list["FakeOntology"] = []

    def __init__(self, *_args, **_kwargs) -> None:
        self.axioms: list[object] = []
        self.created.append(self)

    def add_axiom(self, axiom: object) -> None:
        self.axioms.append(axiom)


def test_abox_projection_does_not_round_trip_through_owlapi_mapper(monkeypatch) -> None:
    ttl = """
        @prefix : <http://example.org/req/0#> .
        @prefix iof: <https://spec.industrialontologies.org/ontology/core/Core/> .
        @prefix owl: <http://www.w3.org/2002/07/owl#> .
        @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

        <http://example.org/req/0#> a owl:Ontology .
        :Req_0 a iof:RequirementSpecification ;
            iof:requirementSatisfiedBy :Design_0 ;
            rdfs:comment "The valve shall control flow." .
        :Design_0 a iof:DesignSpecification .
    """
    graph = Graph().parse(data=ttl, format="turtle")
    builder = AgenticKGBuilder.__new__(AgenticKGBuilder)
    builder._tbox_axioms = ["tbox-axiom"]
    builder._parse_graph_from_text = lambda _text: graph
    FakeOntology.created = []
    monkeypatch.setattr(builder_module, "SyncOntology", FakeOntology)

    builder._update_base_ontology_from_owl(ttl)

    assert len(FakeOntology.created) == 1
    assert builder.base_ontology.axioms[0] == "tbox-axiom"
    assert sum(isinstance(ax, OWLClassAssertionAxiom) for ax in builder._abox_axioms) == 2
    assert sum(isinstance(ax, OWLObjectPropertyAssertionAxiom) for ax in builder._abox_axioms) == 1
    assert sum(isinstance(ax, OWLDataPropertyAssertionAxiom) for ax in builder._abox_axioms) == 1
    assert len(builder._abox_individuals) == 2
    assert len(builder._abox_object_properties) == 1
    assert len(builder._abox_data_properties) == 1


def test_reason_uses_projected_python_abox_axioms(monkeypatch) -> None:
    class FakeReasoner:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def has_consistent_ontology(self) -> bool:
            return True

        def infer_axioms_and_save(self, **_kwargs) -> None:
            pass

    class PoisonBaseOntology:
        def get_abox_axioms(self):
            raise AssertionError("Java ABox enumeration must not be used")

    builder = AgenticKGBuilder.__new__(AgenticKGBuilder)
    builder._tbox_axioms = []
    builder._abox_axioms = ["projected-axiom"]
    builder.base_ontology = PoisonBaseOntology()
    builder.reasoner = "Pellet"
    builder._materialize_inferred_abox_ontology = lambda ontology, reasoner: ontology
    builder._postprocess_inverse_object_properties = lambda ontology: (ontology, 0)
    FakeOntology.created = []
    monkeypatch.setattr(builder_module, "SyncOntology", FakeOntology)
    monkeypatch.setattr(builder_module, "SyncReasoner", FakeReasoner)

    success, message, ontology, _reasoner = builder.reason()

    assert success is True
    assert message == "Pellet reasoning completed."
    assert ontology.axioms == ["projected-axiom"]


def test_grounding_failure_saves_candidate_and_traceback(tmp_path) -> None:
    try:
        error = RuntimeError("conversion failed")
        error.grounding_stage = "abox_projection"
        error.grounding_candidate = "@prefix : <http://example.org/test#> .\n:Req a :Requirement .\n"
        raise error
    except RuntimeError as exc:
        diagnostics = _grounding_failure_diagnostics(exc, tmp_path, idx=7)

    candidate_path = tmp_path / "grounding_candidate_7.ttl"
    assert diagnostics["grounding_stage"] == "abox_projection"
    assert diagnostics["grounding_candidate_path"] == str(candidate_path.resolve())
    assert "RuntimeError: conversion failed" in diagnostics["traceback"]
    assert candidate_path.read_text(encoding="utf-8").startswith("@prefix")
