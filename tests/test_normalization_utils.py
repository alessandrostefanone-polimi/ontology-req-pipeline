from rdflib import Graph, Literal, Namespace, URIRef

from ontology_req_pipeline.normalization.utils import (
    convert_to_SI,
    query_qk_by_unit,
    retrieve_unit_properties,
)


def test_query_qk_by_unit_ignores_invalid_unit_token() -> None:
    graph = Graph()
    assert query_qk_by_unit("?", graph) == []


def test_query_qk_by_unit_matches_quantity_kind_for_valid_unit() -> None:
    graph = Graph()
    qudt = Namespace("http://qudt.org/schema/qudt/")
    pressure_qk = URIRef("http://qudt.org/vocab/quantitykind/Pressure")
    pa_unit = URIRef("http://qudt.org/vocab/unit/PA")
    graph.add((pressure_qk, qudt.applicableUnit, pa_unit))

    result = query_qk_by_unit("pa", graph)
    assert result == [str(pressure_qk)]


def test_retrieve_unit_properties_canonicalizes_bare_qudt_unit_code() -> None:
    graph = Graph()
    qudt = Namespace("http://qudt.org/schema/qudt/")
    milli_m = URIRef("http://qudt.org/vocab/unit/MilliM")
    graph.add((milli_m, qudt.conversionMultiplier, Literal(0.001)))

    properties = retrieve_unit_properties("MilliM", graph)

    assert properties is not None
    assert float(properties["conversionMultiplier"]) == 0.001


def test_convert_to_si_uses_bare_qudt_unit_code() -> None:
    graph = Graph()
    qudt = Namespace("http://qudt.org/schema/qudt/")
    milli_m = URIRef("http://qudt.org/vocab/unit/MilliM")
    meter = URIRef("http://qudt.org/vocab/unit/M")
    graph.add((milli_m, qudt.scalingOf, meter))
    graph.add((milli_m, qudt.conversionMultiplier, Literal(0.001)))
    graph.add((meter, qudt.conversionMultiplier, Literal(1.0)))

    value_si, unit_si = convert_to_SI(300, "MilliM", graph)

    assert value_si == 0.3
    assert str(unit_si) == str(meter)
