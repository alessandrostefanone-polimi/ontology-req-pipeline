"""Deterministic prose/table evidence catalogs for LLM grounding and HITL."""

from __future__ import annotations

import re
from collections.abc import Iterable

from ontology_req_pipeline.document.models import DocumentChunk, EvidenceSpan

SENTENCE_END_RE = re.compile(r"(?<=[.!?])(?:\s+|$)|\n{2,}")
MARKDOWN_SEPARATOR_RE = re.compile(r"^\s*\|?\s*:?-{3,}:?(?:\s*\|\s*:?-{3,}:?)+\s*\|?\s*$")


def _nonempty_segments(text: str) -> Iterable[tuple[int, int, str]]:
    cursor = 0
    for match in SENTENCE_END_RE.finditer(text):
        end = match.start()
        segment = text[cursor:end]
        left = len(segment) - len(segment.lstrip())
        right = len(segment.rstrip())
        if right > left:
            yield cursor + left, cursor + right, segment[left:right]
        cursor = match.end()
    tail = text[cursor:]
    left = len(tail) - len(tail.lstrip())
    right = len(tail.rstrip())
    if right > left:
        yield cursor + left, cursor + right, tail[left:right]


def build_evidence_spans(chunk: DocumentChunk) -> list[EvidenceSpan]:
    raw = chunk.raw_text or chunk.contextualized_text
    spans: list[EvidenceSpan] = []
    table_ranges: list[tuple[int, int]] = []
    offset = 0
    for line in raw.splitlines(keepends=True):
        clean = line.strip()
        start = offset + len(line) - len(line.lstrip())
        end = offset + len(line.rstrip())
        if clean.count("|") >= 2 and not MARKDOWN_SEPARATOR_RE.match(clean):
            table_ranges.append((offset, offset + len(line)))
            spans.append(
                EvidenceSpan(
                    span_id="",
                    kind="TABLE_ROW",
                    text=raw[start:end],
                    start=start,
                    end=end,
                    page_number=chunk.source_locator.page,
                )
            )
        offset += len(line)

    prose_mask = list(raw)
    for start, end in table_ranges:
        for index in range(start, min(end, len(prose_mask))):
            prose_mask[index] = "\n" if prose_mask[index] == "\n" else " "
    prose_text = "".join(prose_mask)
    for start, end, text in _nonempty_segments(prose_text):
        original = raw[start:end]
        if not original.strip() or MARKDOWN_SEPARATOR_RE.match(original.strip()):
            continue
        spans.append(
            EvidenceSpan(
                span_id="",
                kind="PROSE",
                text=original,
                start=start,
                end=end,
                page_number=chunk.source_locator.page,
            )
        )

    spans.sort(key=lambda item: (item.start if item.start is not None else 10**12, item.kind))
    unique: list[EvidenceSpan] = []
    seen: set[tuple[str, int | None, int | None]] = set()
    for span in spans:
        key = (span.text.strip(), span.start, span.end)
        if key in seen:
            continue
        seen.add(key)
        span.span_id = f"E{len(unique) + 1:03d}"
        unique.append(span)
    if not unique and raw.strip():
        start = len(raw) - len(raw.lstrip())
        end = len(raw.rstrip())
        unique.append(
            EvidenceSpan(
                span_id="E001",
                kind="PROSE",
                text=raw[start:end],
                start=start,
                end=end,
                page_number=chunk.source_locator.page,
            )
        )
    return unique


def attach_evidence_catalogs(chunks: list[DocumentChunk]) -> list[DocumentChunk]:
    return [chunk.model_copy(update={"evidence_spans": build_evidence_spans(chunk)}) for chunk in chunks]


def format_evidence_catalog(chunk: DocumentChunk, allowed_ids: list[str] | None = None) -> str:
    allowed = set(allowed_ids or [])
    spans = [span for span in chunk.evidence_spans if not allowed or span.span_id in allowed]
    return "\n".join(
        f"[{span.span_id}] ({span.kind}, page {span.page_number or '?'}) {span.display_text}"
        for span in spans
    )
