"""Typed contracts shared by document extraction, HITL, CLI, and UI."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

Route = Literal["HIGH_CONFIDENCE", "AMBIGUOUS", "LOW_CONFIDENCE", "SKIP_FURNITURE"]


class SourceLocator(BaseModel):
    chunk_id: str
    chunk_index: int
    page: int | None = None
    section: str | None = None


class EvidenceSpan(BaseModel):
    span_id: str
    kind: Literal["PROSE", "TABLE_ROW", "PAGE_IMAGE"] = "PROSE"
    text: str
    display_text: str = ""
    start: int | None = None
    end: int | None = None
    page_number: int | None = None
    bounding_box: list[float] | None = None

    @model_validator(mode="after")
    def populate_display_text(self) -> EvidenceSpan:
        if not self.display_text:
            self.display_text = self.text
        return self


class DocumentChunk(BaseModel):
    model_config = ConfigDict(extra="allow")

    chunk_id: str
    chunk_index: int
    raw_text: str
    contextualized_text: str
    headings: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)
    source_locator: SourceLocator
    requirement_score: float = 0.0
    requirement_score_components: dict[str, float] = Field(default_factory=dict)
    requirement_score_reasons: list[str] = Field(default_factory=list)
    requirement_score_version: str = "rule-v1"
    requirement_route: Route = "LOW_CONFIDENCE"
    evidence_spans: list[EvidenceSpan] = Field(default_factory=list)

    @property
    def text(self) -> str:
        return self.contextualized_text or self.raw_text


class RequirementDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")

    chunk_id: str
    label: Literal["REQUIREMENT", "NOT_REQUIREMENT", "NEEDS_CONTEXT"]
    evidence_span_ids: list[str]
    reason: str = ""
    status: str = "ok"
    route: str = ""

    @model_validator(mode="after")
    def require_evidence_for_positive_decision(self) -> RequirementDecision:
        if self.label == "REQUIREMENT" and not self.evidence_span_ids:
            raise ValueError(
                "REQUIREMENT decisions must include at least one evidence span ID"
            )
        return self


class RequirementDecisionBatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    decisions: list[RequirementDecision]


class GroundedRequirement(BaseModel):
    model_config = ConfigDict(extra="forbid")

    evidence_span_ids: list[str] = Field(min_length=1)
    literal_requirement: str
    normalized_requirement: str
    modality: Literal["SHALL", "MUST", "SHOULD", "MAY", "WILL", "IS", "IMPLICIT"]
    explicitness: Literal["EXPLICIT", "IMPLICIT"]
    context_additions: list[str] = Field(default_factory=list)
    explanation: str = ""


class GroundedRequirementsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    individual_requirements: list[GroundedRequirement]


class CoverageAuditResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    candidate_span_ids: list[str]
    reason: str = ""


class RequirementCandidate(BaseModel):
    model_config = ConfigDict(extra="allow")

    evidence_span_ids: list[str] = Field(default_factory=list)
    literal_requirement: str
    normalized_requirement: str
    modality: str
    explicitness: str
    context_additions: list[str] = Field(default_factory=list)
    explanation: str = ""
    evidence_spans: list[EvidenceSpan] = Field(default_factory=list)
    target_chunk_id: str
    source_chunk_ids: list[str] = Field(default_factory=list)
    route: str
    extraction_pass: str = "INITIAL"
    grounding_status: str = "ok"
    normalization_status: str = "ok"
    requirement_origin: str = "MACHINE_BASELINE"
    requirement_fingerprint: str = ""
    source: dict[str, Any] = Field(default_factory=dict)


class DocumentExtractionConfig(BaseModel):
    provider: Literal["openai", "ollama"] = "ollama"
    model: str = "qwen3.5:9b-bf16"
    high_threshold: float = 35.0
    ambiguous_threshold: float = 15.0
    max_workers: int = 4
    context_radius: int = 1
    table_cell_matching: bool = True
    run_high_confidence: bool = True
    run_ambiguous: bool = True
    run_low_confidence: bool = True
    run_context_followup: bool = True
    run_targeted_coverage_audit: bool = True
    ollama_num_ctx: int = 8192
    ollama_num_predict: int = 3072
    prompt_token_budget: int = 4096
    low_batch_max_tokens: int = 3072
    low_batch_max_chunks: int = 4
    max_chunks: int | None = None

    @model_validator(mode="after")
    def validate_limits(self) -> DocumentExtractionConfig:
        if self.max_workers < 1:
            raise ValueError("max_workers must be at least 1")
        if self.ambiguous_threshold > self.high_threshold:
            raise ValueError("ambiguous_threshold cannot exceed high_threshold")
        if self.max_chunks is not None and self.max_chunks < 1:
            raise ValueError("max_chunks must be at least 1")
        if self.ollama_num_ctx < 1024:
            raise ValueError("ollama_num_ctx must be at least 1024")
        if self.ollama_num_predict < 1:
            raise ValueError("ollama_num_predict must be at least 1")
        if self.prompt_token_budget < 1:
            raise ValueError("prompt_token_budget must be at least 1")
        if self.low_batch_max_tokens < 1:
            raise ValueError("low_batch_max_tokens must be at least 1")
        if self.low_batch_max_tokens > self.prompt_token_budget:
            raise ValueError("low_batch_max_tokens cannot exceed prompt_token_budget")
        if (
            self.provider == "ollama"
            and self.prompt_token_budget + self.ollama_num_predict > self.ollama_num_ctx
        ):
            raise ValueError(
                "prompt_token_budget plus ollama_num_predict cannot exceed ollama_num_ctx"
            )
        return self


class ReviewQueueItem(BaseModel):
    queue_item_id: str
    run_id: str
    document_sha256: str
    chunk_id: str
    chunk_index: int
    page: int | None = None
    priority: Literal["P0", "P1", "P2"]
    reason: str
    requirement_score: float
    requirement_route: str
    machine_requirement_count: int
    machine_requirements: list[dict[str, Any]] = Field(default_factory=list)
    evidence_spans: list[EvidenceSpan] = Field(default_factory=list)
    raw_text: str
    contextualized_text: str


class ReviewDecision(BaseModel):
    decision_id: str
    queue_item_id: str
    run_id: str
    document_sha256: str
    decision: Literal["NO_ADDITION", "ADD_REQUIREMENTS", "DEFERRED"]
    reviewer: str
    selected_evidence_span_ids: list[str] = Field(default_factory=list)
    manual_requirements: list[dict[str, Any]] = Field(default_factory=list)
    notes: str = ""
    reviewed_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class AdjudicationDecision(BaseModel):
    adjudication_decision_id: str
    adjudication_item_id: str
    action: Literal["APPROVE", "EDIT", "REJECT", "DEFERRED"]
    reviewer: str
    normalized_requirement: str | None = None
    modality: str | None = None
    explicitness: str | None = None
    evidence_span_ids: list[str] | None = None
    notes: str = ""
    reviewed_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class RunManifest(BaseModel):
    run_id: str
    created_at: str
    updated_at: str
    status: str
    document_sha256: str
    source_path: str
    stored_pdf_path: str
    output_dir: str
    config: dict[str, Any]
    counts: dict[str, int] = Field(default_factory=dict)
    artifacts: dict[str, str] = Field(default_factory=dict)
    errors: list[dict[str, Any]] = Field(default_factory=list)

    @classmethod
    def new(
        cls,
        *,
        run_id: str,
        document_sha256: str,
        source_path: Path,
        stored_pdf_path: Path,
        output_dir: Path,
        config: DocumentExtractionConfig,
    ) -> RunManifest:
        now = datetime.now(timezone.utc).isoformat()
        return cls(
            run_id=run_id,
            created_at=now,
            updated_at=now,
            status="created",
            document_sha256=document_sha256,
            source_path=str(source_path.resolve()),
            stored_pdf_path=str(stored_pdf_path.resolve()),
            output_dir=str(output_dir.resolve()),
            config=config.model_dump(mode="json"),
        )
