from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any, TypeVar

import pytest
from pydantic import BaseModel, ValidationError

from ontology_req_pipeline.document.evidence import attach_evidence_catalogs
from ontology_req_pipeline.document.extraction import (
    DocumentRequirementExtractor,
    ModelClient,
    _batch_triage_prompt,
    _chunks_in_batches,
    _structured_prompt_tokens,
)
from ontology_req_pipeline.document.models import (
    CoverageAuditResponse,
    DocumentChunk,
    DocumentExtractionConfig,
    GroundedRequirement,
    GroundedRequirementsResponse,
    RequirementDecision,
    RequirementDecisionBatch,
    SourceLocator,
)
from ontology_req_pipeline.document.scoring import score_chunks
from ontology_req_pipeline.extraction.utils import OllamaStructuredResponseError

T = TypeVar("T", bound=BaseModel)


class FakeStructuredClient:
    def parse(self, output_model: type[T], prompt: str) -> T:
        if output_model is RequirementDecision:
            return output_model(
                chunk_id="chunk-0",
                label="REQUIREMENT",
                evidence_span_ids=["E001"],
                reason="Normative statement.",
            )
        if output_model is RequirementDecisionBatch:
            return output_model(decisions=[])
        if output_model is CoverageAuditResponse:
            return output_model(candidate_span_ids=[])
        if output_model is GroundedRequirementsResponse:
            return output_model(
                individual_requirements=[
                    GroundedRequirement(
                        evidence_span_ids=["E001"],
                        literal_requirement="The pump shall withstand 10 bar.",
                        normalized_requirement="The pump shall withstand 10 bar.",
                        modality="SHALL",
                        explicitness="EXPLICIT",
                    )
                ]
            )
        raise AssertionError(output_model)


def test_high_confidence_extraction_is_evidence_grounded() -> None:
    chunk = DocumentChunk(
        chunk_id="chunk-0",
        chunk_index=0,
        raw_text="The pump shall withstand 10 bar.",
        contextualized_text="The pump shall withstand 10 bar.",
        source_locator=SourceLocator(chunk_id="chunk-0", chunk_index=0, page=1),
    )
    chunks = attach_evidence_catalogs(score_chunks([chunk]))
    extractor = DocumentRequirementExtractor(
        DocumentExtractionConfig(max_workers=1, run_targeted_coverage_audit=False),
        client=FakeStructuredClient(),
    )

    candidates, decisions, debug = extractor.run(chunks)

    assert decisions == []
    assert len(candidates) == 1
    assert candidates[0].evidence_span_ids == ["E001"]
    assert candidates[0].source["source_locator"]["page"] == 1
    assert candidates[0].requirement_fingerprint.startswith("REQFP-")
    assert debug[0]["status"] == "ok"


class FailingStructuredClient:
    def parse(self, output_model: type[T], prompt: str) -> T:
        cause = ValueError("required field is missing")
        error = OllamaStructuredResponseError(
            "local-model",
            [
                {
                    "attempt": 1,
                    "error_type": "ValidationError",
                    "error": "required field is missing",
                    "done_reason": "stop",
                    "raw_response_length": 18,
                    "raw_response": '{"unexpected": 1}',
                    "raw_response_truncated": False,
                }
            ],
        )
        raise error from cause


def test_llm_debug_event_includes_provider_and_root_cause_diagnostics() -> None:
    extractor = DocumentRequirementExtractor(
        DocumentExtractionConfig(max_workers=1),
        client=FailingStructuredClient(),
    )

    result = extractor._call(
        GroundedRequirementsResponse,
        "prompt",
        route="EXTRACTION_INITIAL",
        chunk_ids=["chunk-50"],
    )

    assert result is None
    event = extractor.debug_events[0]
    assert event["status"] == "error"
    assert event["cause_type"] == "ValueError"
    assert event["cause_error"] == "required field is missing"
    assert event["provider_diagnostics"][0]["done_reason"] == "stop"
    assert event["provider_diagnostics"][0]["raw_response"] == '{"unexpected": 1}'


def _chunk(
    chunk_id: str, index: int, text: str = "Background information."
) -> DocumentChunk:
    chunk = DocumentChunk(
        chunk_id=chunk_id,
        chunk_index=index,
        raw_text=text,
        contextualized_text=text,
        source_locator=SourceLocator(chunk_id=chunk_id, chunk_index=index, page=1),
    )
    return attach_evidence_catalogs([chunk])[0]


def test_grounded_requirement_requires_nonempty_evidence_ids() -> None:
    with pytest.raises(ValidationError, match="evidence_span_ids"):
        GroundedRequirement(
            literal_requirement="The unit shall operate.",
            normalized_requirement="The unit shall operate.",
            modality="SHALL",
            explicitness="EXPLICIT",
        )

    schema = GroundedRequirement.model_json_schema()
    assert "evidence_span_ids" in schema["required"]
    assert schema["properties"]["evidence_span_ids"]["minItems"] == 1


def test_positive_triage_requires_evidence_and_context_budget_is_consistent() -> None:
    with pytest.raises(ValidationError, match="evidence_span_ids"):
        RequirementDecision(
            chunk_id="chunk-0",
            label="REQUIREMENT",
            evidence_span_ids=[],
        )

    with pytest.raises(ValidationError, match="cannot exceed ollama_num_ctx"):
        DocumentExtractionConfig(
            ollama_num_ctx=8192,
            ollama_num_predict=5000,
            prompt_token_budget=4096,
        )


class RepairingStructuredClient:
    def __init__(self) -> None:
        self.prompts: list[str] = []

    def parse(self, output_model: type[T], prompt: str) -> T:
        self.prompts.append(prompt)
        assert output_model is GroundedRequirementsResponse
        evidence_id = "E001" if "CORRECTION REQUIRED" in prompt else "E999"
        return output_model(
            individual_requirements=[
                GroundedRequirement(
                    evidence_span_ids=[evidence_id],
                    literal_requirement="The unit shall operate.",
                    normalized_requirement="The unit shall operate.",
                    modality="SHALL",
                    explicitness="EXPLICIT",
                )
            ]
        )


def test_materialization_rejection_is_logged_and_repaired() -> None:
    client = RepairingStructuredClient()
    chunk = _chunk("chunk-0", 0, "The unit shall operate.")
    extractor = DocumentRequirementExtractor(
        DocumentExtractionConfig(max_workers=1, run_targeted_coverage_audit=False),
        client=client,
    )

    results = extractor._extract_allowed(
        chunk,
        [],
        ["E001"],
        "INITIAL",
        "initial extraction",
    )

    assert len(results) == 1
    assert results[0].evidence_span_ids == ["E001"]
    assert len(client.prompts) == 2
    rejection = next(
        event
        for event in extractor.debug_events
        if event["event_type"] == "materialization_rejection"
    )
    assert rejection["reason"] == "evidence_span_ids_outside_allowed_scope"
    assert any(
        event["route"] == "EXTRACTION_INITIAL_VALIDATION_REPAIR"
        for event in extractor.debug_events
    )


class RecordingClient:
    def __init__(self) -> None:
        self.calls = 0

    def parse(self, output_model: type[T], prompt: str) -> T:
        self.calls += 1
        raise AssertionError("over-budget prompts must not reach the provider")


def test_prompt_budget_blocks_oversized_single_chunk() -> None:
    client = RecordingClient()
    extractor = DocumentRequirementExtractor(
        DocumentExtractionConfig(
            max_workers=1,
            prompt_token_budget=1,
            low_batch_max_tokens=1,
            ollama_num_predict=1,
        ),
        client=client,
    )

    result = extractor._call(
        GroundedRequirementsResponse,
        "oversized prompt",
        route="EXTRACTION_INITIAL",
        chunk_ids=["chunk-0"],
    )

    assert result is None
    assert client.calls == 0
    assert extractor.debug_events[0]["error_type"] == "PromptBudgetExceeded"


def test_low_confidence_batches_use_rendered_prompt_token_budget() -> None:
    chunks = [_chunk(f"chunk-{index}", index, "Background " * 80) for index in range(3)]
    one_tokens = _structured_prompt_tokens(
        RequirementDecisionBatch,
        _batch_triage_prompt(chunks[:1]),
    )
    two_tokens = _structured_prompt_tokens(
        RequirementDecisionBatch,
        _batch_triage_prompt(chunks[:2]),
    )
    assert two_tokens > one_tokens

    batches = _chunks_in_batches(
        chunks,
        max_tokens=(one_tokens + two_tokens) // 2,
        max_chunks=4,
    )

    assert [[chunk.chunk_id for chunk in batch] for batch in batches] == [
        ["chunk-0"],
        ["chunk-1"],
        ["chunk-2"],
    ]


class FakeNativeClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def chat(self, **kwargs: Any):
        self.calls.append(kwargs)
        return SimpleNamespace(
            message=SimpleNamespace(content=json.dumps({"value": "ok"}))
        )


class ValueResponse(BaseModel):
    value: str


def test_model_client_sets_explicit_ollama_context_and_output_limits() -> None:
    native = FakeNativeClient()
    client = ModelClient.__new__(ModelClient)
    client.provider = "ollama"
    client.model = "local-model"
    client.ollama_num_ctx = 16384
    client.ollama_num_predict = 2048
    client.client = native

    result = client.parse(ValueResponse, "prompt")

    assert result.value == "ok"
    assert native.calls[0]["think"] is False
    assert native.calls[0]["options"] == {
        "temperature": 0.0,
        "num_ctx": 16384,
        "num_predict": 2048,
    }
