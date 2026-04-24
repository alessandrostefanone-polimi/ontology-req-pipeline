from rdflib import URIRef
from rdflib.namespace import RDF, RDFS

from ontology_req_pipeline.data_models import Condition, IndividualRequirement, Record, Span, Structure
from ontology_req_pipeline.ontology.template_instantiation import requirement_to_rdf


def test_requirement_to_rdf_creates_requirement_node() -> None:
    record = Record(
        idx=42,
        original_text="The pump shall operate at 10 bar.",
        requirements=[
            IndividualRequirement(
                req_idx=0,
                raw_text="The pump shall operate at 10 bar.",
                structure=Structure(
                    subject=Span(text="The pump", start=0, end=8),
                    modality="shall",
                    condition=Condition(),
                    action=Span(text="shall operate", start=9, end=21),
                    object=Span(text="at 10 bar", start=22, end=31),
                ),
                constraints=[],
                references=[],
            )
        ],
    )

    graph = requirement_to_rdf(record)
    assert len(graph) > 0

    req_uri = URIRef("http://example.org/req/42#Req_0")
    req_spec_uri = URIRef("https://spec.industrialontologies.org/ontology/core/Core/RequirementSpecification")
    assert (req_uri, RDF.type, req_spec_uri) in graph


def test_requirement_to_rdf_includes_text_comment() -> None:
    record = Record(
        idx=8,
        original_text="The valve shall close within 2 seconds.",
        requirements=[
            IndividualRequirement(
                req_idx=0,
                raw_text="The valve shall close within 2 seconds.",
                structure=Structure(
                    subject=Span(text="The valve", start=0, end=9),
                    modality="shall",
                    condition=Condition(),
                    action=Span(text="shall close", start=10, end=21),
                    object=Span(text="within 2 seconds", start=22, end=38),
                ),
                constraints=[],
                references=[],
            )
        ],
    )

    graph = requirement_to_rdf(record)
    comments = list(graph.triples((None, RDFS.comment, None)))
    assert any("close within 2 seconds" in str(obj) for _, _, obj in comments)
