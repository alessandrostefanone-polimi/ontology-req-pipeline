"""Rule-based extraction baseline aligned with the repository Record schema."""

from __future__ import annotations

from functools import lru_cache
import re
from typing import Iterable, Optional

from ontology_req_pipeline.data_models import (
    Attribute,
    Condition,
    Constraint,
    Group,
    IndividualRequirement,
    Qualifiers,
    Record,
    Span,
    Structure,
    Target,
    Value,
    ValueQuantity,
)
from ontology_req_pipeline.extraction.extractor_base import BaseExtractor

MODALITY_WORDS = ("shall", "must", "should", "may", "will", "is")
CONDITION_MARKERS = ("if", "when", "while", "during", "unless", "before", "after", "once")
OBJECT_DEPS = {"dobj", "obj", "attr", "oprd", "xcomp", "ccomp", "pobj"}
SUBJECT_DEPS = {"nsubj", "nsubjpass"}
SUSPICIOUS_UNIT_NAMES = {"dollar", "centavo", "farad", "turn"}

SECTION_REF_RE = re.compile(r"\b(?:[A-Z]{1,3}\.)\s*\d+(?:\.\d+)+\b")
DOTTED_SECTION_RE = re.compile(r"\b\d+\.\d+(?:\.\d+)+\b")
ALL_CAPS_HEADING_RE = re.compile(r"\b[A-Z]{2,}(?:\s+[A-Z]{2,})+\b")

CONSTRAINT_PATTERNS = [
    (
        "between",
        re.compile(r"\bbetween\b.+?\band\b.+?(?=(?:,|;|\.|$))", re.IGNORECASE),
    ),
    (
        "ge",
        re.compile(
            r"(?:\b(?:at least|minimum of|minimum|no less than|not less than)\b.+?(?=(?:,|;|\.|$)))|(?:\b\d+(?:\.\d+)?\s*(?:%|[A-Za-z]+(?:\s*\([^)]+\))?)\s+or more\b)",
            re.IGNORECASE,
        ),
    ),
    (
        "le",
        re.compile(
            r"(?:\b(?:at most|maximum of|maximum|no more than|not more than|up to)\b.+?(?=(?:,|;|\.|$)))|(?:\b\d+(?:\.\d+)?\s*(?:%|[A-Za-z]+(?:\s*\([^)]+\))?)\s+or less\b)",
            re.IGNORECASE,
        ),
    ),
    ("lt", re.compile(r"\b(?:less than|below|under)\b.+?(?=(?:,|;|\.|$))", re.IGNORECASE)),
    ("gt", re.compile(r"\b(?:greater than|more than|above|over)\b.+?(?=(?:,|;|\.|$))", re.IGNORECASE)),
    ("eq", re.compile(r"\b(?:exactly|equal to)\b.+?(?=(?:,|;|\.|$))", re.IGNORECASE)),
]


def _require_spacy():
    try:
        import spacy
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError(
            "Rule-based extraction requires spaCy. Install it with `pip install spacy`."
        ) from exc
    return spacy


def _require_quantulum():
    try:
        from quantulum3 import parser as quantulum_parser
    except ImportError as exc:  # pragma: no cover - dependency guard
        raise RuntimeError(
            "Rule-based extraction requires quantulum3. Install it with `pip install quantulum3`."
        ) from exc
    return quantulum_parser


@lru_cache(maxsize=1)
def _load_nlp():
    spacy = _require_spacy()
    model_name = "en_core_web_sm"
    try:
        return spacy.load(model_name)
    except OSError as exc:  # pragma: no cover - model guard
        raise RuntimeError(
            f"spaCy model '{model_name}' is not installed. Run `python -m spacy download {model_name}`."
        ) from exc


def placeholder_span() -> Span:
    return Span(text="", start=0, end=0)


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip(" -:;,.\n\t")


def make_span(text: str, start: int, end: int) -> Span:
    start = max(0, start)
    end = max(start, end)
    return Span(text=text[start:end], start=start, end=end)


def span_from_tokens(doc, tokens: Iterable) -> Span:
    tokens = list(tokens)
    if not tokens:
        return placeholder_span()
    start = min(tok.idx for tok in tokens)
    end = max(tok.idx + len(tok.text) for tok in tokens)
    return make_span(doc.text, start, end)


def subtree_span(token) -> Span:
    return span_from_tokens(token.doc, list(token.subtree))


def normalize_requirement_text(raw_text: str) -> str:
    text = raw_text
    text = text.replace(r"\%", "%")
    text = text.replace("$", "")
    text = text.replace("Â±", "+-")
    text = text.replace("Ã‚Â±", "+-")
    text = text.replace("##", " ")
    text = SECTION_REF_RE.sub(" ", text)
    text = DOTTED_SECTION_RE.sub(" ", text)
    text = re.sub(r"\bFigure\s+\d+\b", " ", text, flags=re.IGNORECASE)
    text = ALL_CAPS_HEADING_RE.sub(" ", text)
    text = re.sub(r"\b[a-zA-Z]\.\s+", " ", text)
    text = re.sub(r"^\s*\d+\s+", "", text)
    text = re.sub(r"\s+", " ", text)
    return clean_text(text)


def split_requirement_candidates(text: str) -> list[str]:
    candidates = re.split(
        r"(?<=[.!?])\s+|(?<=:)\s+(?=[A-Z])|\s+(?=(?:[A-Z][a-z].*?\b(?:shall|must|should|may|will|is)\b))",
        text,
    )
    cleaned = [clean_text(candidate) for candidate in candidates if clean_text(candidate)]
    return cleaned or [clean_text(text)]


def score_candidate(candidate: str) -> tuple[int, int]:
    lower = candidate.lower()
    modal_hits = sum(int(word in lower) for word in MODALITY_WORDS)
    comparator_hits = sum(
        int(word in lower)
        for word in [
            "less than",
            "more than",
            "at least",
            "at most",
            "minimum",
            "maximum",
            "between",
            " or less",
            " or more",
        ]
    )
    heading_penalty = candidate.count("##") + len(SECTION_REF_RE.findall(candidate))
    return (modal_hits * 10 + comparator_hits * 3 - heading_penalty * 4, len(candidate))


def normalize_negated_comparatives(text: str) -> str:
    normalized = text
    normalized = re.sub(
        r"\bmust\s+not\s+be\s+more\s+than\b",
        "must be at most",
        normalized,
        flags=re.IGNORECASE,
    )
    normalized = re.sub(
        r"\bmust\s+not\s+be\s+less\s+than\b",
        "must be at least",
        normalized,
        flags=re.IGNORECASE,
    )
    normalized = re.sub(r"\bnot\s+more\s+than\b", "at most", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\bnot\s+less\s+than\b", "at least", normalized, flags=re.IGNORECASE)
    return normalized


def prepare_requirement_candidates(raw_text: str) -> list[str]:
    normalized = normalize_requirement_text(raw_text)
    normalized = normalize_negated_comparatives(normalized)
    candidates = split_requirement_candidates(normalized)
    cleaned = []
    for candidate in candidates:
        candidate = re.sub(
            r"^(?:Wheel Size|Applicability|Personnel|Maintenance|Width|Position)\s+",
            "",
            candidate,
            flags=re.IGNORECASE,
        )
        candidate = clean_text(candidate)
        if candidate:
            cleaned.append(candidate)
    cleaned = sorted(cleaned, key=score_candidate, reverse=True)
    return cleaned or [clean_text(normalized)]


def find_modality(doc) -> str:
    for token in doc:
        if token.lower_ in MODALITY_WORDS:
            return token.lower_
    return "is"


def find_action_head(doc):
    for token in doc:
        if token.lower_ in MODALITY_WORDS and token.head != token and token.head.pos_ in {"VERB", "AUX"}:
            return token.head
    root = doc[:].root
    if root.pos_ in {"VERB", "AUX", "ADJ", "NOUN"}:
        return root
    for token in doc:
        if token.pos_ in {"VERB", "AUX"}:
            return token
    return root


def find_subject_span(action_head) -> Span:
    for child in action_head.children:
        if child.dep_ in SUBJECT_DEPS:
            return subtree_span(child)
    for token in action_head.doc:
        if token.dep_ in SUBJECT_DEPS:
            return subtree_span(token)
    noun_chunks = [chunk for chunk in action_head.doc.noun_chunks if chunk.end_char <= action_head.idx]
    if noun_chunks:
        chunk = noun_chunks[-1]
        return make_span(action_head.doc.text, chunk.start_char, chunk.end_char)
    return placeholder_span()


def find_object_span(action_head) -> Span:
    for child in action_head.children:
        if child.dep_ in OBJECT_DEPS:
            return subtree_span(child)
    rights = [
        tok
        for tok in action_head.subtree
        if tok.i > action_head.i and tok.pos_ in {"NOUN", "PROPN", "ADJ", "NUM"}
    ]
    if rights:
        return span_from_tokens(action_head.doc, rights)
    return placeholder_span()


def find_action_span(action_head) -> Span:
    keep = [action_head]
    for child in action_head.children:
        if child.dep_ in {"aux", "auxpass", "neg", "prt", "advmod", "acomp", "attr"}:
            keep.append(child)
    return span_from_tokens(action_head.doc, sorted(set(keep), key=lambda tok: tok.i))


def find_condition(doc) -> Condition:
    text = doc.text
    for marker in CONDITION_MARKERS:
        match = re.search(rf"\b{marker}\b.+?(?=(?:,|;|\.|$))", text, re.IGNORECASE)
        if match:
            return Condition(
                present=True,
                text=clean_text(match.group(0)),
                start=match.start(),
                end=match.end(),
            )
    return Condition(present=False, text="", start=0, end=0)


def infer_attribute_name(doc, evidence_start: int) -> str:
    candidates = []
    for chunk in doc.noun_chunks:
        chunk_text = clean_text(chunk.text)
        if not chunk_text or any(char.isdigit() for char in chunk_text[:4]):
            continue
        if chunk.end_char <= evidence_start:
            distance = evidence_start - chunk.end_char
            candidates.append((distance, chunk_text))
    if candidates:
        candidates.sort(key=lambda item: item[0])
        return candidates[0][1]
    return "quantitative attribute"


def sanitize_quantity_text(snippet: str) -> str:
    cleaned = snippet
    cleaned = cleaned.replace(r"\%", "%")
    cleaned = cleaned.replace("$", "")
    cleaned = SECTION_REF_RE.sub(" ", cleaned)
    cleaned = DOTTED_SECTION_RE.sub(" ", cleaned)
    cleaned = re.sub(r"\b(?:[A-Z]|[a-z])\b\s*$", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    return clean_text(cleaned)


def parse_quantities_safe(snippet: str):
    parser = _require_quantulum()
    try:
        return parser.parse(sanitize_quantity_text(snippet))
    except Exception:
        return []


def canonical_unit_text(parsed_quantity, snippet: str) -> Optional[str]:
    snippet_lower = snippet.lower()
    if "%" in snippet_lower or "percent" in snippet_lower or "percentage" in snippet_lower:
        return "percent"
    if re.search(r"\bmm\b", snippet_lower):
        return "millimetre"
    if re.search(r"\bcm\b", snippet_lower):
        return "centimetre"
    if re.search(r"\bkg\b", snippet_lower):
        return "kilogram"
    if re.search(r"\bkpa\b", snippet_lower):
        return "kilopascal"
    if re.search(r"\bms\b", snippet_lower):
        return "millisecond"
    if re.search(r"\binch(?:es)?\b", snippet_lower):
        return "inch"
    unit = getattr(parsed_quantity, "unit", None)
    if unit is None:
        return None
    if unit.name in SUSPICIOUS_UNIT_NAMES:
        return None
    return unit.name


def build_quantity_value(operator_name: str, quantities: list, snippet: str) -> ValueQuantity:
    q1 = quantities[0]
    unit_text = canonical_unit_text(q1, snippet)
    if operator_name == "between" and len(quantities) >= 2:
        q2 = quantities[1]
        return ValueQuantity(v1=float(q1.value), v2=float(q2.value), tol=None, unit_text=unit_text)
    uncertainty = getattr(q1, "uncertainty", None)
    tol = float(uncertainty) if uncertainty is not None else None
    return ValueQuantity(v1=float(q1.value), v2=None, tol=tol, unit_text=unit_text)


def spans_overlap(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
    return not (a_end <= b_start or a_start >= b_end)


def regex_constraint_matches(text: str):
    accepted = []
    for operator_name, pattern in CONSTRAINT_PATTERNS:
        for match in pattern.finditer(text):
            start, end = match.start(), match.end()
            if any(spans_overlap(start, end, prev_start, prev_end) for prev_start, prev_end in accepted):
                continue
            accepted.append((start, end))
            yield operator_name, match.group(0), start, end


def quantity_is_noise(full_text: str, quantity, allow_dimensionless: bool = False) -> bool:
    start, end = quantity.span
    surface = clean_text(quantity.surface)
    if not surface:
        return True
    before = full_text[max(0, start - 2):start]
    after = full_text[end:min(len(full_text), end + 2)]
    if "." in before or "." in after:
        return True
    if SECTION_REF_RE.search(surface) or DOTTED_SECTION_RE.search(surface):
        return True
    if re.fullmatch(r"\d+(?:\.\d+)?", surface) and not allow_dimensionless:
        return True
    unit = getattr(quantity, "unit", None)
    if unit is not None and unit.name in SUSPICIOUS_UNIT_NAMES and "%" not in surface:
        return True
    return False


def extract_constraints(text: str, doc, object_span: Span) -> list[Constraint]:
    constraints = []
    used_char_spans = []

    for operator_name, snippet, start, end in regex_constraint_matches(text):
        cleaned_snippet = sanitize_quantity_text(snippet)
        quantities = [
            q
            for q in parse_quantities_safe(cleaned_snippet)
            if not quantity_is_noise(cleaned_snippet, q, allow_dimensionless=True)
        ]
        if not quantities:
            continue
        value_quantity = build_quantity_value(operator_name, quantities, cleaned_snippet)
        constraints.append(
            Constraint(
                constraint_idx=len(constraints),
                group=Group(group_id=f"g{len(constraints)}", relation="AND"),
                target=Target(kind="object", ref=object_span.text or None),
                evidence=make_span(text, start, end),
                attribute=Attribute(name=infer_attribute_name(doc, start), kind="quantity"),
                operator=operator_name,
                value=Value(kind="quantity", raw_text=cleaned_snippet, quantity=value_quantity),
                depends_on=None,
                qualifiers=Qualifiers(),
            )
        )
        used_char_spans.append((start, end))

    fallback_quantities = parse_quantities_safe(text)
    for quantity in fallback_quantities:
        start, end = quantity.span
        if any(spans_overlap(start, end, used_start, used_end) for used_start, used_end in used_char_spans):
            continue
        if quantity_is_noise(text, quantity, allow_dimensionless=False):
            continue
        snippet = sanitize_quantity_text(text[start:end])
        value_quantity = build_quantity_value("eq", [quantity], snippet)
        constraints.append(
            Constraint(
                constraint_idx=len(constraints),
                group=Group(group_id=f"g{len(constraints)}", relation="AND"),
                target=Target(kind="object", ref=object_span.text or None),
                evidence=make_span(text, start, end),
                attribute=Attribute(name=infer_attribute_name(doc, start), kind="quantity"),
                operator="eq",
                value=Value(kind="quantity", raw_text=snippet, quantity=value_quantity),
                depends_on=None,
                qualifiers=Qualifiers(),
            )
        )

    return constraints


def extract_rule_based_record(text: str, idx: int = 0, candidate_rank: int = 0) -> Record:
    candidates = prepare_requirement_candidates(text)
    chosen_rank = min(max(candidate_rank, 0), len(candidates) - 1)
    prepared_text = candidates[chosen_rank]
    doc = _load_nlp()(prepared_text)
    action_head = find_action_head(doc)
    object_span = find_object_span(action_head)
    req = IndividualRequirement(
        req_idx=0,
        raw_text=prepared_text,
        structure=Structure(
            subject=find_subject_span(action_head),
            modality=find_modality(doc),
            condition=find_condition(doc),
            action=find_action_span(action_head),
            object=object_span,
        ),
        constraints=extract_constraints(prepared_text, doc, object_span),
        references=[],
    )
    return Record(idx=int(idx), original_text=prepared_text, requirements=[req])


class RuleBasedExtractor(BaseExtractor):
    """spaCy + regex + quantulum3 extraction baseline."""

    def extract(
        self,
        text: str,
        local: bool = False,
        idx: Optional[str] = "demo",
        model: Optional[str] = None,
        candidate_rank: int = 0,
        **_: object,
    ) -> Record:
        del local, model
        return extract_rule_based_record(text=text, idx=int(idx or 0), candidate_rank=candidate_rank)
