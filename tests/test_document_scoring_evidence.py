from __future__ import annotations

from ontology_req_pipeline.document.evidence import build_evidence_spans
from ontology_req_pipeline.document.models import DocumentChunk, SourceLocator
from ontology_req_pipeline.document.scoring import score_chunk


def _chunk(text: str, *, headings: list[str] | None = None) -> DocumentChunk:
    return DocumentChunk(
        chunk_id="chunk-0",
        chunk_index=0,
        raw_text=text,
        contextualized_text=text,
        headings=headings or [],
        source_locator=SourceLocator(chunk_id="chunk-0", chunk_index=0, page=2),
    )


def test_normative_quantity_chunk_routes_high_confidence() -> None:
    scored = score_chunk(_chunk("The pump shall deliver at least 10 L per minute."))

    assert scored.requirement_route == "HIGH_CONFIDENCE"
    assert "NORMATIVE_MODAL" in scored.requirement_score_components
    assert "CONSTRAINT_PHRASE" in scored.requirement_score_components


def test_evidence_catalog_preserves_table_rows_and_offsets() -> None:
    text = "The enclosure shall be sealed.\n\n| Parameter | Limit |\n| --- | --- |\n| Pressure | 10 bar |\n"
    chunk = _chunk(text)
    spans = build_evidence_spans(chunk)

    assert any(span.kind == "PROSE" and "shall be sealed" in span.text for span in spans)
    table_span = next(span for span in spans if span.kind == "TABLE_ROW" and "Pressure" in span.text)
    assert text[table_span.start : table_span.end] == table_span.text
    assert table_span.page_number == 2
    assert len({span.span_id for span in spans}) == len(spans)
