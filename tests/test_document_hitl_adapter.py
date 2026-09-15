from __future__ import annotations

from pathlib import Path

from ontology_req_pipeline.document.adapter import to_pipeline_rows
from ontology_req_pipeline.document.artifacts import DocumentRunPaths, write_json
from ontology_req_pipeline.document.evidence import attach_evidence_catalogs
from ontology_req_pipeline.document.hitl import (
    apply_review,
    finalize_adjudication,
    prepare_adjudication,
    prepare_review,
    save_adjudication_decision,
    save_review_decision,
)
from ontology_req_pipeline.document.models import (
    DocumentChunk,
    RequirementCandidate,
    SourceLocator,
)


def _paths(tmp_path: Path) -> DocumentRunPaths:
    paths = DocumentRunPaths(tmp_path / "run")
    paths.ensure()
    pdf = paths.source_dir / "source.pdf"
    pdf.write_bytes(b"%PDF-1.4\n%%EOF")
    write_json(
        paths.manifest,
        {
            "run_id": "run-1",
            "document_sha256": "a" * 64,
            "source_path": str(pdf),
            "stored_pdf_path": str(pdf),
            "output_dir": str(paths.root),
            "status": "created",
            "counts": {},
            "artifacts": {},
            "errors": [],
            "config": {},
        },
    )
    return paths


def test_hitl_manual_recovery_adjudication_and_adapter(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    chunk = DocumentChunk(
        chunk_id="chunk-0",
        chunk_index=0,
        raw_text="The valve shall close. It shall report status.",
        contextualized_text="The valve shall close. It shall report status.",
        source_locator=SourceLocator(chunk_id="chunk-0", chunk_index=0, page=3),
        requirement_score=40,
        requirement_route="HIGH_CONFIDENCE",
    )
    chunk = attach_evidence_catalogs([chunk])[0]
    machine = RequirementCandidate(
        evidence_span_ids=["E001"],
        literal_requirement="The valve shall close.",
        normalized_requirement="The valve shall close.",
        modality="SHALL",
        explicitness="EXPLICIT",
        evidence_spans=[chunk.evidence_spans[0]],
        target_chunk_id="chunk-0",
        source_chunk_ids=["chunk-0"],
        route="HIGH_CONFIDENCE",
        source={"source_locator": chunk.source_locator.model_dump(mode="json")},
    )
    queue = prepare_review(paths, [chunk], [machine], [])
    second_span = chunk.evidence_spans[1]
    save_review_decision(
        paths,
        queue[0].queue_item_id,
        "ADD_REQUIREMENTS",
        reviewer="reviewer-1",
        selected_evidence_span_ids=[second_span.span_id],
        manual_requirements=[
            {
                "literal_requirement": "It shall report status.",
                "normalized_requirement": "The valve shall report status.",
                "modality": "SHALL",
                "explicitness": "EXPLICIT",
                "evidence_span_ids": [second_span.span_id],
            }
        ],
    )

    applied = apply_review(paths)
    assert len(applied["requirements"]) == 2
    assert len(applied["recovered"]) == 1

    adjudication_queue = prepare_adjudication(paths)
    assert len(adjudication_queue) == 2
    for item in adjudication_queue:
        save_adjudication_decision(
            paths,
            item["adjudication_item_id"],
            "APPROVE",
            reviewer="reviewer-1",
        )
    adjudicated = finalize_adjudication(paths)
    assert len(adjudicated["requirements"]) == 2

    rows = to_pipeline_rows(paths, source="adjudicated")
    assert [row["idx"] for row in rows] == [0, 1]
    assert rows[0]["source"]["doc_id"] == "a" * 64
    assert rows[0]["source"]["page"] == 3
    assert rows[0]["source"]["requirement_fingerprint"].startswith("REQFP-")
