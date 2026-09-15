from __future__ import annotations

from pathlib import Path
from typing import TypeVar

from pydantic import BaseModel

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
from ontology_req_pipeline.document.service import DocumentPipelineService

T = TypeVar("T", bound=BaseModel)


class FakeClient:
    def parse(self, output_model: type[T], prompt: str) -> T:
        if output_model is GroundedRequirementsResponse:
            return output_model(
                individual_requirements=[
                    GroundedRequirement(
                        evidence_span_ids=["E001"],
                        literal_requirement="The unit shall operate.",
                        normalized_requirement="The unit shall operate.",
                        modality="SHALL",
                        explicitness="EXPLICIT",
                    )
                ]
            )
        if output_model is CoverageAuditResponse:
            return output_model(candidate_span_ids=[])
        if output_model is RequirementDecisionBatch:
            return output_model(decisions=[])
        if output_model is RequirementDecision:
            return output_model(chunk_id="chunk-0", label="REQUIREMENT", evidence_span_ids=["E001"])
        raise AssertionError(output_model)


def test_document_service_creates_resumable_artifacts(tmp_path: Path, monkeypatch) -> None:
    source = tmp_path / "input.pdf"
    source.write_bytes(b"%PDF-1.4\n%%EOF")
    chunk = DocumentChunk(
        chunk_id="chunk-0",
        chunk_index=0,
        raw_text="The unit shall operate.",
        contextualized_text="The unit shall operate.",
        source_locator=SourceLocator(chunk_id="chunk-0", chunk_index=0, page=1),
    )
    monkeypatch.setattr(
        "ontology_req_pipeline.document.service.ingest_pdf", lambda *args, **kwargs: [chunk]
    )
    service = DocumentPipelineService(client=FakeClient())
    config = DocumentExtractionConfig(
        max_workers=1,
        max_chunks=1,
        run_targeted_coverage_audit=False,
    )

    result = service.create_and_extract(source, tmp_path / "runs", config)

    paths = result["paths"]
    assert paths.manifest.exists()
    assert (paths.extraction_dir / "chunks.jsonl").exists()
    assert (paths.hitl_dir / "review_queue.jsonl").exists()
    assert len(result["requirements"]) == 1
