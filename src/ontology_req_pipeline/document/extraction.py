"""Route-aware, evidence-grounded requirement discovery for PDF chunks."""

from __future__ import annotations

import json
import re
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from typing import Any, Protocol, TypeVar

from pydantic import BaseModel

from ontology_req_pipeline.document.evidence import format_evidence_catalog
from ontology_req_pipeline.document.models import (
    CoverageAuditResponse,
    DocumentChunk,
    DocumentExtractionConfig,
    GroundedRequirement,
    GroundedRequirementsResponse,
    RequirementCandidate,
    RequirementDecision,
    RequirementDecisionBatch,
)

T = TypeVar("T", bound=BaseModel)
ProgressCallback = Callable[[str, int, int, str], None]


class StructuredClient(Protocol):
    def parse(self, output_model: type[T], prompt: str) -> T: ...


class ModelClient:
    """Small provider adapter using the project's existing structured-output helpers."""

    def __init__(
        self,
        provider: str,
        model: str,
        *,
        ollama_num_ctx: int = 8192,
        ollama_num_predict: int = 3072,
    ):
        self.provider = provider.strip().lower()
        self.model = model
        self.ollama_num_ctx = ollama_num_ctx
        self.ollama_num_predict = ollama_num_predict
        if self.provider == "ollama":
            from ollama import Client

            self.client: Any = Client()
        elif self.provider == "openai":
            from openai import OpenAI

            self.client = OpenAI()
        else:
            raise ValueError("provider must be 'openai' or 'ollama'")

    def parse(self, output_model: type[T], prompt: str) -> T:
        from ontology_req_pipeline.extraction.utils import run_ollama, run_openai

        if self.provider == "ollama":
            return run_ollama(
                self.client,
                prompt,
                output_model=output_model,
                model=self.model,
                options={
                    "temperature": 0.0,
                    "num_ctx": self.ollama_num_ctx,
                    "num_predict": self.ollama_num_predict,
                },
            )
        return run_openai(
            self.client, prompt, output_model=output_model, model=self.model
        )


def _context_chunks(
    chunk: DocumentChunk, chunks: list[DocumentChunk], radius: int
) -> list[DocumentChunk]:
    lower = max(0, chunk.chunk_index - radius)
    upper = min(len(chunks), chunk.chunk_index + radius + 1)
    return [
        candidate
        for candidate in chunks[lower:upper]
        if candidate.chunk_id != chunk.chunk_id
    ]


def _context_text(chunks: list[DocumentChunk]) -> str:
    if not chunks:
        return "(none)"
    return "\n\n".join(
        f"[{chunk.chunk_id}; page {chunk.source_locator.page or '?'}]\n{chunk.text}"
        for chunk in chunks
    )


def _triage_prompt(chunk: DocumentChunk, context: list[DocumentChunk]) -> str:
    return f"""You are a technical requirements recall reviewer.

Classify only the TARGET chunk as REQUIREMENT, NOT_REQUIREMENT, or NEEDS_CONTEXT.
A requirement is an obligation, capability, constraint, performance target, interface condition,
acceptance criterion, or prohibition. Tables may express implicit requirements without a modal.
Use context only to resolve inherited subject/modality or boundaries. Do not treat context as target evidence.
If REQUIREMENT, return one or more evidence_span_ids from the target catalog. Return JSON only.

TARGET CHUNK ID: {chunk.chunk_id}
ROUTE SCORE: {chunk.requirement_score}
TARGET EVIDENCE:
{format_evidence_catalog(chunk)}

NEIGHBOR CONTEXT:
{_context_text(context)}
"""


def _batch_triage_prompt(chunks: list[DocumentChunk]) -> str:
    body = "\n\n".join(
        f"TARGET {chunk.chunk_id}; score={chunk.requirement_score}\n{format_evidence_catalog(chunk)}"
        for chunk in chunks
    )
    return f"""Classify every target chunk independently as REQUIREMENT, NOT_REQUIREMENT, or NEEDS_CONTEXT.
Technical requirements include explicit obligations and implicit table acceptance criteria. Definitions,
navigation, history, examples, and explanatory prose are not requirements unless normative. Evidence IDs
must come from the corresponding target. Return exactly one decision per chunk as JSON.

{body}
"""


def _extraction_prompt(
    chunk: DocumentChunk,
    context: list[DocumentChunk],
    allowed_ids: list[str] | None = None,
    purpose: str = "initial extraction",
    validation_feedback: str | None = None,
) -> str:
    allowed = allowed_ids or [span.span_id for span in chunk.evidence_spans]
    repair = ""
    if validation_feedback:
        repair = f"""

CORRECTION REQUIRED:
The previous candidate output was rejected by deterministic evidence validation:
{validation_feedback}
Return corrected requirements only. Every evidence_span_ids value must be present and must use only the
allowed IDs shown below.
"""
    return f"""You are an expert systems engineer extracting atomic technical requirements.

PURPOSE: {purpose}
Extract every requirement supported by the ALLOWED TARGET EVIDENCE. Produce one output per independent
obligation. Never invent a requirement. Context may supply an inherited actor, subject, modality, or
sentence boundary, but it cannot supply an independent requirement.

For each output:
- evidence_span_ids must be a non-empty subset of the allowed IDs;
- literal_requirement closely transcribes the evidence;
- normalized_requirement may repair grammar and inherited context but preserves actor, modality,
  negation, conditions, values, units, and references;
- modality is SHALL, MUST, SHOULD, MAY, WILL, IS, or IMPLICIT;
- explicitness is EXPLICIT when the target states a normative modal, otherwise IMPLICIT;
- list every insertion taken from neighbor context in context_additions.
Return JSON only.
{repair}

TARGET: {chunk.chunk_id}; page {chunk.source_locator.page or "?"}
ALLOWED TARGET EVIDENCE:
{format_evidence_catalog(chunk, allowed)}

NEIGHBOR CONTEXT:
{_context_text(context)}
"""


def _coverage_prompt(
    chunk: DocumentChunk,
    context: list[DocumentChunk],
    requirements: list[RequirementCandidate],
    eligible_ids: list[str],
) -> str:
    existing = (
        "\n".join(f"- {item.normalized_requirement}" for item in requirements)
        or "(none)"
    )
    return f"""Audit the eligible evidence spans for missed technical requirements.
Return only span IDs that support at least one requirement not already represented below. Do not select
definitions, examples, headings, or background. Context only resolves inheritance. Return JSON only.

TARGET: {chunk.chunk_id}
ELIGIBLE EVIDENCE:
{format_evidence_catalog(chunk, eligible_ids)}

EXISTING REQUIREMENTS:
{existing}

CONTEXT:
{_context_text(context)}
"""


TOKEN_PIECE_RE = re.compile(r"\w+|[^\w\s]", re.UNICODE)


def _estimate_tokens(text: str) -> int:
    """Return a conservative tokenizer-independent estimate for local model budgeting."""

    byte_estimate = (len(text.encode("utf-8")) + 3) // 4
    piece_estimate = len(TOKEN_PIECE_RE.findall(text))
    return max(1, byte_estimate, piece_estimate)


def _structured_prompt_tokens(output_model: type[BaseModel], prompt: str) -> int:
    from ontology_req_pipeline.extraction.utils import OLLAMA_STRUCTURED_SYSTEM_PROMPT

    schema = json.dumps(
        output_model.model_json_schema(), ensure_ascii=False, separators=(",", ":")
    )
    return (
        _estimate_tokens(f"{OLLAMA_STRUCTURED_SYSTEM_PROMPT}\n{prompt}\n{schema}") + 16
    )


def _chunks_in_batches(
    chunks: list[DocumentChunk], max_tokens: int, max_chunks: int
) -> list[list[DocumentChunk]]:
    batches: list[list[DocumentChunk]] = []
    current: list[DocumentChunk] = []
    for chunk in chunks:
        candidate = [*current, chunk]
        candidate_tokens = _structured_prompt_tokens(
            RequirementDecisionBatch,
            _batch_triage_prompt(candidate),
        )
        if current and (len(current) >= max_chunks or candidate_tokens > max_tokens):
            batches.append(current)
            current = [chunk]
        else:
            current = candidate
    if current:
        batches.append(current)
    return batches


def _fingerprint(
    chunk_id: str, evidence_ids: list[str], normalized_requirement: str
) -> str:
    canonical = json.dumps(
        [
            chunk_id,
            sorted(evidence_ids),
            " ".join(normalized_requirement.lower().split()),
        ],
        ensure_ascii=False,
    )
    return "REQFP-" + sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _deduplicate(candidates: list[RequirementCandidate]) -> list[RequirementCandidate]:
    seen: set[str] = set()
    results: list[RequirementCandidate] = []
    for candidate in candidates:
        fingerprint = candidate.requirement_fingerprint or _fingerprint(
            candidate.target_chunk_id,
            candidate.evidence_span_ids,
            candidate.normalized_requirement,
        )
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        results.append(
            candidate.model_copy(update={"requirement_fingerprint": fingerprint})
        )
    return results


def _materialize(
    requirement: GroundedRequirement,
    chunk: DocumentChunk,
    context: list[DocumentChunk],
    extraction_pass: str,
) -> RequirementCandidate | None:
    evidence_by_id = {span.span_id: span for span in chunk.evidence_spans}
    ids = list(dict.fromkeys(requirement.evidence_span_ids))
    if not ids or any(span_id not in evidence_by_id for span_id in ids):
        return None
    normalized = " ".join(requirement.normalized_requirement.split()).strip()
    literal = " ".join(requirement.literal_requirement.split()).strip()
    if not normalized or not literal:
        return None
    evidence = [evidence_by_id[span_id] for span_id in ids]
    source_chunk_ids = [chunk.chunk_id] + [item.chunk_id for item in context]
    source_quotes = [item.text for item in evidence]
    fingerprint = _fingerprint(chunk.chunk_id, ids, normalized)
    return RequirementCandidate(
        evidence_span_ids=ids,
        literal_requirement=literal,
        normalized_requirement=normalized,
        modality=requirement.modality,
        explicitness=requirement.explicitness,
        context_additions=requirement.context_additions,
        explanation=requirement.explanation,
        evidence_spans=evidence,
        target_chunk_id=chunk.chunk_id,
        source_chunk_ids=source_chunk_ids,
        route=chunk.requirement_route,
        extraction_pass=extraction_pass,
        requirement_fingerprint=fingerprint,
        source={
            "target_chunk_id": chunk.chunk_id,
            "source_locator": chunk.source_locator.model_dump(mode="json"),
            "raw_text": chunk.raw_text,
            "source_quotes": source_quotes,
        },
    )


COVERAGE_CUE_RE = re.compile(
    r"\b(?:shall|must|required|may only|prohibited|minimum|maximum|at least|at most|acceptance|criterion|tolerance)\b|\d\s*(?:%|mm|cm|m|kg|g|s|ms|v|a|w|pa|bar|hz|rpm)\b",
    re.IGNORECASE,
)


class DocumentRequirementExtractor:
    def __init__(
        self, config: DocumentExtractionConfig, client: StructuredClient | None = None
    ):
        self.config = config
        self.client = client or ModelClient(
            config.provider,
            config.model,
            ollama_num_ctx=config.ollama_num_ctx,
            ollama_num_predict=config.ollama_num_predict,
        )
        self.debug_events: list[dict[str, Any]] = []
        self._debug_lock = threading.Lock()

    def _call(
        self,
        output_model: type[T],
        prompt: str,
        *,
        route: str,
        chunk_ids: list[str],
        token_budget: int | None = None,
    ) -> T | None:
        estimated_tokens = _structured_prompt_tokens(output_model, prompt)
        selected_budget = token_budget or self.config.prompt_token_budget
        if estimated_tokens > selected_budget:
            event = {
                "status": "error",
                "event_type": "llm_call",
                "route": route,
                "chunk_ids": chunk_ids,
                "error_type": "PromptBudgetExceeded",
                "error": (
                    f"Estimated prompt size {estimated_tokens} exceeds configured budget "
                    f"{selected_budget}."
                ),
                "estimated_prompt_tokens": estimated_tokens,
                "prompt_token_budget": selected_budget,
            }
            with self._debug_lock:
                self.debug_events.append(event)
            return None
        started = time.perf_counter()
        try:
            result = self.client.parse(output_model, prompt)
            event = {
                "status": "ok",
                "event_type": "llm_call",
                "route": route,
                "chunk_ids": chunk_ids,
                "estimated_prompt_tokens": estimated_tokens,
                "elapsed_seconds": round(time.perf_counter() - started, 3),
            }
            with self._debug_lock:
                self.debug_events.append(event)
            return result
        except Exception as exc:  # noqa: BLE001
            event = {
                "status": "error",
                "event_type": "llm_call",
                "route": route,
                "chunk_ids": chunk_ids,
                "error_type": type(exc).__name__,
                "error": str(exc),
                "estimated_prompt_tokens": estimated_tokens,
                "elapsed_seconds": round(time.perf_counter() - started, 3),
            }
            provider_diagnostics = getattr(exc, "provider_diagnostics", None)
            if provider_diagnostics:
                event["provider_diagnostics"] = provider_diagnostics
            cause = exc.__cause__
            if cause is not None:
                event["cause_type"] = type(cause).__name__
                event["cause_error"] = str(cause)
            with self._debug_lock:
                self.debug_events.append(event)
            return None

    def _record_materialization_rejection(
        self,
        *,
        chunk: DocumentChunk,
        extraction_pass: str,
        allowed_ids: list[str],
        requirement: GroundedRequirement,
        reason: str,
    ) -> None:
        with self._debug_lock:
            self.debug_events.append(
                {
                    "status": "warning",
                    "event_type": "materialization_rejection",
                    "route": f"EXTRACTION_{extraction_pass}",
                    "chunk_ids": [chunk.chunk_id],
                    "reason": reason,
                    "allowed_evidence_span_ids": allowed_ids,
                    "returned_evidence_span_ids": requirement.evidence_span_ids,
                    "literal_requirement": requirement.literal_requirement,
                    "normalized_requirement": requirement.normalized_requirement,
                }
            )

    def _materialize_response(
        self,
        *,
        parsed: GroundedRequirementsResponse,
        chunk: DocumentChunk,
        context: list[DocumentChunk],
        allowed_ids: list[str],
        extraction_pass: str,
    ) -> tuple[list[RequirementCandidate], list[dict[str, Any]]]:
        allowed = set(allowed_ids)
        results: list[RequirementCandidate] = []
        rejections: list[dict[str, Any]] = []
        for requirement in parsed.individual_requirements:
            returned_ids = set(requirement.evidence_span_ids)
            reason: str | None = None
            if not returned_ids:
                reason = "missing_evidence_span_ids"
            elif not returned_ids.issubset(allowed):
                reason = "evidence_span_ids_outside_allowed_scope"
            elif not requirement.literal_requirement.strip():
                reason = "empty_literal_requirement"
            elif not requirement.normalized_requirement.strip():
                reason = "empty_normalized_requirement"

            item = None
            if reason is None:
                item = _materialize(requirement, chunk, context, extraction_pass)
                if item is None:
                    reason = "materialization_failed"
            if reason is not None:
                self._record_materialization_rejection(
                    chunk=chunk,
                    extraction_pass=extraction_pass,
                    allowed_ids=allowed_ids,
                    requirement=requirement,
                    reason=reason,
                )
                rejections.append(
                    {
                        "reason": reason,
                        "candidate": requirement.model_dump(mode="json"),
                    }
                )
            elif item is not None:
                results.append(item)
        return results, rejections

    def _scope_decision(
        self,
        chunk: DocumentChunk,
        decision: RequirementDecision,
    ) -> RequirementDecision:
        valid_ids = {span.span_id for span in chunk.evidence_spans}
        ids = [
            span_id for span_id in decision.evidence_span_ids if span_id in valid_ids
        ]
        if ids != decision.evidence_span_ids:
            with self._debug_lock:
                self.debug_events.append(
                    {
                        "status": "warning",
                        "event_type": "triage_evidence_scope_validation",
                        "route": chunk.requirement_route,
                        "chunk_ids": [chunk.chunk_id],
                        "valid_evidence_span_ids": sorted(valid_ids),
                        "returned_evidence_span_ids": decision.evidence_span_ids,
                    }
                )
        if decision.label == "REQUIREMENT" and not ids:
            return RequirementDecision(
                chunk_id=chunk.chunk_id,
                label="NEEDS_CONTEXT",
                evidence_span_ids=[],
                reason="Positive triage decision did not contain a valid target evidence span ID.",
                status="ROUTING_ERROR",
                route=chunk.requirement_route,
            )
        return decision.model_copy(
            update={"evidence_span_ids": ids, "route": chunk.requirement_route}
        )

    def _triage_one(
        self, chunk: DocumentChunk, chunks: list[DocumentChunk]
    ) -> RequirementDecision:
        context = _context_chunks(chunk, chunks, self.config.context_radius)
        decision = self._call(
            RequirementDecision,
            _triage_prompt(chunk, context),
            route=chunk.requirement_route,
            chunk_ids=[chunk.chunk_id] + [item.chunk_id for item in context],
        )
        if decision is None:
            return RequirementDecision(
                chunk_id=chunk.chunk_id,
                label="NEEDS_CONTEXT",
                evidence_span_ids=[],
                reason="Structured triage failed after provider retries.",
                status="LLM_ERROR",
                route=chunk.requirement_route,
            )
        if decision.chunk_id != chunk.chunk_id:
            return RequirementDecision(
                chunk_id=chunk.chunk_id,
                label="NEEDS_CONTEXT",
                evidence_span_ids=[],
                reason=f"Model returned wrong chunk ID: {decision.chunk_id}",
                status="ROUTING_ERROR",
                route=chunk.requirement_route,
            )
        return self._scope_decision(chunk, decision)

    def triage(
        self, chunks: list[DocumentChunk], progress: ProgressCallback | None = None
    ) -> list[RequirementDecision]:
        ambiguous = [
            item
            for item in chunks
            if item.requirement_route == "AMBIGUOUS" and self.config.run_ambiguous
        ]
        low = [
            item
            for item in chunks
            if item.requirement_route == "LOW_CONFIDENCE"
            and self.config.run_low_confidence
        ]
        decisions: list[RequirementDecision] = []

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            for index, decision in enumerate(
                executor.map(lambda item: self._triage_one(item, chunks), ambiguous)
            ):
                decisions.append(decision)
                if progress:
                    progress(
                        "triage",
                        index + 1,
                        len(ambiguous) + len(low),
                        decision.chunk_id,
                    )

        batches = _chunks_in_batches(
            low,
            self.config.low_batch_max_tokens,
            self.config.low_batch_max_chunks,
        )
        processed = len(ambiguous)
        for batch in batches:
            parsed = self._call(
                RequirementDecisionBatch,
                _batch_triage_prompt(batch),
                route="LOW_CONFIDENCE_BATCH",
                chunk_ids=[item.chunk_id for item in batch],
                token_budget=self.config.low_batch_max_tokens,
            )
            returned = (
                {item.chunk_id: item for item in parsed.decisions} if parsed else {}
            )
            for chunk in batch:
                decision = returned.get(chunk.chunk_id)
                if decision is None:
                    decision = RequirementDecision(
                        chunk_id=chunk.chunk_id,
                        label="NEEDS_CONTEXT",
                        evidence_span_ids=[],
                        reason="No valid decision returned in low-confidence batch.",
                        status="LLM_ERROR" if parsed is None else "ROUTING_ERROR",
                    )
                decision = self._scope_decision(chunk, decision).model_copy(
                    update={"route": "LOW_CONFIDENCE"}
                )
                decisions.append(decision)
                processed += 1
                if progress:
                    progress(
                        "triage", processed, len(ambiguous) + len(low), chunk.chunk_id
                    )

        if self.config.run_context_followup:
            by_id = {item.chunk_id: item for item in chunks}
            followups = [
                item
                for item in decisions
                if item.label == "NEEDS_CONTEXT" and item.chunk_id in by_id
            ]
            replacements = {
                decision.chunk_id: decision
                for decision in (
                    self._triage_one(by_id[item.chunk_id], chunks) for item in followups
                )
            }
            decisions = [replacements.get(item.chunk_id, item) for item in decisions]
        return decisions

    def _extract_allowed(
        self,
        chunk: DocumentChunk,
        context: list[DocumentChunk],
        allowed_ids: list[str],
        extraction_pass: str,
        purpose: str,
    ) -> list[RequirementCandidate]:
        if not allowed_ids:
            return []
        parsed = self._call(
            GroundedRequirementsResponse,
            _extraction_prompt(chunk, context, allowed_ids, purpose),
            route=f"EXTRACTION_{extraction_pass}",
            chunk_ids=[chunk.chunk_id] + [item.chunk_id for item in context],
        )
        if parsed is None:
            return []
        results, rejections = self._materialize_response(
            parsed=parsed,
            chunk=chunk,
            context=context,
            allowed_ids=allowed_ids,
            extraction_pass=extraction_pass,
        )
        if rejections:
            feedback = json.dumps(rejections, ensure_ascii=False)
            if len(feedback) > 6_000:
                feedback = feedback[:6_000] + "...[validation feedback truncated]"
            repaired = self._call(
                GroundedRequirementsResponse,
                _extraction_prompt(
                    chunk,
                    context,
                    allowed_ids,
                    purpose,
                    validation_feedback=feedback,
                ),
                route=f"EXTRACTION_{extraction_pass}_VALIDATION_REPAIR",
                chunk_ids=[chunk.chunk_id] + [item.chunk_id for item in context],
            )
            if repaired is not None:
                repaired_results, _ = self._materialize_response(
                    parsed=repaired,
                    chunk=chunk,
                    context=context,
                    allowed_ids=allowed_ids,
                    extraction_pass=f"{extraction_pass}_VALIDATION_REPAIR",
                )
                results.extend(repaired_results)
        return _deduplicate(results)

    def _extract_one(
        self, chunk: DocumentChunk, chunks: list[DocumentChunk], use_context: bool
    ) -> list[RequirementCandidate]:
        context = (
            _context_chunks(chunk, chunks, self.config.context_radius)
            if use_context
            else []
        )
        all_ids = [span.span_id for span in chunk.evidence_spans]
        results = self._extract_allowed(
            chunk, context, all_ids, "INITIAL", "initial extraction"
        )
        if not results:
            results = self._extract_allowed(
                chunk,
                context,
                all_ids,
                "RECONCILIATION",
                "positive-route empty-output reconciliation",
            )
            return results
        if not self.config.run_targeted_coverage_audit:
            return results

        covered = {span_id for item in results for span_id in item.evidence_span_ids}
        eligible = [
            span.span_id
            for span in chunk.evidence_spans
            if span.span_id not in covered and COVERAGE_CUE_RE.search(span.text)
        ]
        if not eligible:
            return results
        audit = self._call(
            CoverageAuditResponse,
            _coverage_prompt(chunk, context, results, eligible),
            route="COVERAGE_AUDIT",
            chunk_ids=[chunk.chunk_id] + [item.chunk_id for item in context],
        )
        if audit is None:
            return results
        selected = [
            span_id for span_id in audit.candidate_span_ids if span_id in set(eligible)
        ]
        results.extend(
            self._extract_allowed(
                chunk,
                context,
                selected,
                "TARGETED_COVERAGE",
                "targeted missed-requirement coverage",
            )
        )
        return _deduplicate(results)

    def extract(
        self,
        chunks: list[DocumentChunk],
        decisions: list[RequirementDecision],
        progress: ProgressCallback | None = None,
    ) -> list[RequirementCandidate]:
        positive_ids = {
            item.chunk_id for item in decisions if item.label == "REQUIREMENT"
        }
        selected: list[tuple[DocumentChunk, bool]] = []
        if self.config.run_high_confidence:
            selected.extend(
                (item, False)
                for item in chunks
                if item.requirement_route == "HIGH_CONFIDENCE"
            )
        selected.extend(
            (item, True)
            for item in chunks
            if item.chunk_id in positive_ids
            and item.requirement_route != "HIGH_CONFIDENCE"
        )
        candidates: list[RequirementCandidate] = []
        total = len(selected)

        def extract_item(
            value: tuple[DocumentChunk, bool],
        ) -> list[RequirementCandidate]:
            return self._extract_one(value[0], chunks, value[1])

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            for index, results in enumerate(
                executor.map(extract_item, selected), start=1
            ):
                candidates.extend(results)
                if progress:
                    progress(
                        "extraction", index, total, selected[index - 1][0].chunk_id
                    )
        return _deduplicate(candidates)

    def run(
        self,
        chunks: list[DocumentChunk],
        progress: ProgressCallback | None = None,
    ) -> tuple[
        list[RequirementCandidate], list[RequirementDecision], list[dict[str, Any]]
    ]:
        decisions = self.triage(chunks, progress=progress)
        candidates = self.extract(chunks, decisions, progress=progress)
        return candidates, decisions, list(self.debug_events)


def recover_requirements_for_review(
    *,
    chunk: DocumentChunk,
    all_chunks: list[DocumentChunk],
    selected_evidence_span_ids: list[str],
    config: DocumentExtractionConfig,
    client: StructuredClient | None = None,
) -> list[RequirementCandidate]:
    """Run evidence-constrained recovery for one human-selected source chunk."""

    extractor = DocumentRequirementExtractor(config, client=client)
    context = _context_chunks(chunk, all_chunks, config.context_radius)
    results = extractor._extract_allowed(
        chunk,
        context,
        selected_evidence_span_ids,
        "HITL_RECOVERY",
        "human-selected recall recovery",
    )
    return [
        item.model_copy(update={"requirement_origin": "HITL_LLM_RECOVERY"})
        for item in results
    ]
