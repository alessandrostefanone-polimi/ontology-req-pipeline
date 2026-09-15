"""Auditable high-recall scoring and routing for document chunks."""

from __future__ import annotations

import re
import unicodedata

from ontology_req_pipeline.document.models import DocumentChunk

REQUIREMENT_SCORE_VERSION = "rule-v1"
DEFAULT_HIGH_THRESHOLD = 35.0
DEFAULT_AMBIGUOUS_THRESHOLD = 15.0

SCORE_WEIGHTS = {
    "NORMATIVE_MODAL": 20.0,
    "PROHIBITION": 24.0,
    "WEAK_MODAL": 5.0,
    "NUMBER_UNIT": 14.0,
    "CONSTRAINT_PHRASE": 8.0,
    "NUMBER": 2.0,
    "MEASUREMENT_UNIT": 4.0,
    "FUNCTIONAL_BEHAVIOR": 8.0,
    "REQUIREMENT_ID": 6.0,
    "REQUIREMENT_HEADING": 6.0,
    "INHERITED_MODALITY": 10.0,
    "TABLE_PARAMETER_ROW": 5.0,
}
PENALTIES = {
    "INFORMATIVE_CUE": -10.0,
    "REVISION_OR_TOC": -15.0,
    "DEFINITION_ONLY": -6.0,
}

NORMATIVE_MODAL_RE = re.compile(r"\b(?:shall|must|is required to|required to)\b", re.IGNORECASE)
PROHIBITION_RE = re.compile(r"\b(?:shall not|must not|may only|is prohibited from)\b", re.IGNORECASE)
WEAK_MODAL_RE = re.compile(r"\b(?:should|will)\b", re.IGNORECASE)
NUMBER_RE = re.compile(r"(?<![\w.])[-+]?(?:\d+(?:[.,]\d+)?|\.\d+)(?![\w.])")
UNIT_TOKEN = r"%|°\s?[cf]|mm|cm|km|µm|um|m|in|ft|yd|nm|ns|us|ms|s|h|hours?|minutes?|mins?|kg|g|mg|lb|n|kn|pa|kpa|mpa|bar|v|kv|mv|a|ma|w|kw|j|hz|db|rpm|rad|mol|l|ml"
UNIT_RE = re.compile(rf"(?<![\w.])(?:{UNIT_TOKEN})(?![\w])", re.IGNORECASE)
NUMBER_UNIT_RE = re.compile(
    rf"(?<![\w.])[-+]?(?:\d+(?:[.,]\d+)?|\.\d+)(?:\s*(?:to|[-–—])\s*[-+]?(?:\d+(?:[.,]\d+)?|\.\d+))?\s*(?:{UNIT_TOKEN})(?![\w])",
    re.IGNORECASE,
)
RANGE_RE = re.compile(
    r"\b(?:minimum|maximum|min\.?|max\.?|at least|at most|no more than|no less than|not less than|not greater than|within|tolerance|between|range|up to)\b|(?<![\w])[-+]?\d+(?:[.,]\d+)?\s*(?:to|[-–—])\s*[-+]?\d+(?:[.,]\d+)?",
    re.IGNORECASE,
)
REQUIREMENT_ID_RE = re.compile(r"\b(?:REQ|FR|NFR|SR|PR|DR)[-_ ]?\d{1,}[A-Z]?\b", re.IGNORECASE)
HEADING_RE = re.compile(
    r"\b(?:requirements?|functional|performance|safety|interface|constraint|verification|acceptance|environmental|reliability|operational)\b",
    re.IGNORECASE,
)
INFORMATIVE_RE = re.compile(r"\b(?:informative|background|example|rationale|bibliography)\b", re.IGNORECASE)
REVISION_TOC_RE = re.compile(r"\b(?:revision history|change log|table of contents|superseding|amendment)\b", re.IGNORECASE)
DEFINITION_ONLY_RE = re.compile(r"^\s*[^.!?]{1,160}\s+(?:means|is defined as)\b", re.IGNORECASE)
TABLE_HEADER_RE = re.compile(r"\b(?:parameter|requirement|limit|unit|tolerance|criterion|threshold)\b", re.IGNORECASE)
FUNCTIONAL_VERBS = (
    "accept", "activate", "calculate", "check", "classify", "communicate", "compare", "control",
    "convert", "detect", "determine", "disable", "display", "enable", "enter", "generate", "identify",
    "isolate", "log", "measure", "monitor", "notify", "prevent", "process", "provide", "record",
    "reject", "report", "reset", "respond", "sample", "send", "store", "support", "switch",
    "transmit", "trigger", "update", "verify",
)
FUNCTIONAL_BEHAVIOR_RE = re.compile(
    r"\b(?:shall|must|should|will|required to)\b[^.!?;]{0,90}\b(?:"
    + "|".join(FUNCTIONAL_VERBS)
    + r")\b",
    re.IGNORECASE,
)


def _normalize(value: str) -> str:
    return unicodedata.normalize("NFKC", value or "").replace("–", "-").replace("—", "-")


def score_chunk(
    chunk: DocumentChunk,
    *,
    high_threshold: float = DEFAULT_HIGH_THRESHOLD,
    ambiguous_threshold: float = DEFAULT_AMBIGUOUS_THRESHOLD,
) -> DocumentChunk:
    body = _normalize(chunk.text)
    headings = _normalize(" > ".join(chunk.headings))
    all_text = f"{headings}\n{body}"
    context_type = str(chunk.metadata.get("context_type", "")).lower()
    if context_type in {"furniture", "header", "footer"} or chunk.metadata.get("is_furniture"):
        return chunk.model_copy(
            update={
                "requirement_score": 0.0,
                "requirement_score_reasons": ["HEADER_FOOTER"],
                "requirement_score_version": REQUIREMENT_SCORE_VERSION,
                "requirement_route": "SKIP_FURNITURE",
            }
        )

    components: dict[str, float] = {}

    def add(reason: str, weight: float) -> None:
        components.setdefault(reason, float(weight))

    if NORMATIVE_MODAL_RE.search(body):
        add("NORMATIVE_MODAL", SCORE_WEIGHTS["NORMATIVE_MODAL"])
    if PROHIBITION_RE.search(body):
        add("PROHIBITION", SCORE_WEIGHTS["PROHIBITION"])
    if WEAK_MODAL_RE.search(body) and not NORMATIVE_MODAL_RE.search(body):
        add("WEAK_MODAL", SCORE_WEIGHTS["WEAK_MODAL"])
    if NUMBER_UNIT_RE.search(body):
        add("NUMBER_UNIT", SCORE_WEIGHTS["NUMBER_UNIT"])
    if RANGE_RE.search(body):
        add("CONSTRAINT_PHRASE", SCORE_WEIGHTS["CONSTRAINT_PHRASE"])
    if NUMBER_RE.search(body) and not re.search(r"\b(?:19|20)\d{2}\b", body):
        add("NUMBER", SCORE_WEIGHTS["NUMBER"])
    if UNIT_RE.search(body):
        add("MEASUREMENT_UNIT", SCORE_WEIGHTS["MEASUREMENT_UNIT"])
    if FUNCTIONAL_BEHAVIOR_RE.search(body):
        add("FUNCTIONAL_BEHAVIOR", SCORE_WEIGHTS["FUNCTIONAL_BEHAVIOR"])
    if REQUIREMENT_ID_RE.search(body):
        add("REQUIREMENT_ID", SCORE_WEIGHTS["REQUIREMENT_ID"])
    if HEADING_RE.search(headings):
        add("REQUIREMENT_HEADING", SCORE_WEIGHTS["REQUIREMENT_HEADING"])
    if NORMATIVE_MODAL_RE.search(str(chunk.metadata.get("lead_in", ""))):
        add("INHERITED_MODALITY", SCORE_WEIGHTS["INHERITED_MODALITY"])
    if context_type in {"table", "table_row"} and TABLE_HEADER_RE.search(
        str(chunk.metadata.get("table_headers", ""))
    ):
        add("TABLE_PARAMETER_ROW", SCORE_WEIGHTS["TABLE_PARAMETER_ROW"])
    if INFORMATIVE_RE.search(all_text):
        add("INFORMATIVE_CUE", PENALTIES["INFORMATIVE_CUE"])
    if REVISION_TOC_RE.search(all_text):
        add("REVISION_OR_TOC", PENALTIES["REVISION_OR_TOC"])
    if DEFINITION_ONLY_RE.search(body):
        add("DEFINITION_ONLY", PENALTIES["DEFINITION_ONLY"])

    score = round(max(0.0, min(100.0, sum(components.values()))), 2)
    if score >= high_threshold:
        route = "HIGH_CONFIDENCE"
    elif score >= ambiguous_threshold:
        route = "AMBIGUOUS"
    else:
        route = "LOW_CONFIDENCE"
    return chunk.model_copy(
        update={
            "requirement_score": score,
            "requirement_score_components": components,
            "requirement_score_reasons": list(components),
            "requirement_score_version": REQUIREMENT_SCORE_VERSION,
            "requirement_route": route,
        }
    )


def score_chunks(
    chunks: list[DocumentChunk],
    *,
    high_threshold: float = DEFAULT_HIGH_THRESHOLD,
    ambiguous_threshold: float = DEFAULT_AMBIGUOUS_THRESHOLD,
) -> list[DocumentChunk]:
    return [
        score_chunk(chunk, high_threshold=high_threshold, ambiguous_threshold=ambiguous_threshold)
        for chunk in chunks
    ]
