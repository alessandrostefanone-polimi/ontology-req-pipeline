"""Application service orchestrating PDF extraction, review, and downstream runs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from ontology_req_pipeline.document.adapter import (
    RequirementSource,
    write_pipeline_input,
)
from ontology_req_pipeline.document.artifacts import (
    DocumentRunPaths,
    create_run,
    read_json,
    read_jsonl,
    update_manifest,
    write_jsonl,
)
from ontology_req_pipeline.document.evidence import attach_evidence_catalogs
from ontology_req_pipeline.document.extraction import (
    DocumentRequirementExtractor,
    ModelClient,
    ProgressCallback,
    StructuredClient,
    recover_requirements_for_review,
)
from ontology_req_pipeline.document.hitl import (
    apply_review,
    finalize_adjudication,
    prepare_adjudication,
    prepare_review,
    save_adjudication_decision,
    save_review_decision,
)
from ontology_req_pipeline.document.ingestion import ingest_pdf
from ontology_req_pipeline.document.models import (
    DocumentChunk,
    DocumentExtractionConfig,
)
from ontology_req_pipeline.document.scoring import score_chunks


class DocumentPipelineService:
    def __init__(self, client: StructuredClient | None = None):
        self.client = client

    def create_run(
        self,
        pdf_path: Path,
        output_root: Path,
        config: DocumentExtractionConfig,
        run_name: str | None = None,
    ) -> DocumentRunPaths:
        return create_run(pdf_path, output_root, config, run_name=run_name)

    def extract(
        self,
        paths: DocumentRunPaths,
        *,
        config: DocumentExtractionConfig | None = None,
        progress: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        manifest = read_json(paths.manifest, {}) or {}
        selected_config = config or DocumentExtractionConfig.model_validate(
            manifest.get("config", {})
        )
        update_manifest(paths, status="ingesting")
        try:
            chunks = ingest_pdf(
                paths.pdf,
                table_cell_matching=selected_config.table_cell_matching,
                max_chunks=selected_config.max_chunks,
                progress=progress,
            )
            chunks = score_chunks(
                chunks,
                high_threshold=selected_config.high_threshold,
                ambiguous_threshold=selected_config.ambiguous_threshold,
            )
            chunks = attach_evidence_catalogs(chunks)
            write_jsonl(
                paths.extraction_dir / "chunks.jsonl",
                (chunk.model_dump(mode="json") for chunk in chunks),
            )
            update_manifest(
                paths,
                status="extracting_requirements",
                counts={
                    "source_chunk_count": len(chunks),
                    "high_chunk_count": sum(
                        item.requirement_route == "HIGH_CONFIDENCE" for item in chunks
                    ),
                    "ambiguous_chunk_count": sum(
                        item.requirement_route == "AMBIGUOUS" for item in chunks
                    ),
                    "low_chunk_count": sum(
                        item.requirement_route == "LOW_CONFIDENCE" for item in chunks
                    ),
                },
                artifacts={
                    "chunks": str((paths.extraction_dir / "chunks.jsonl").resolve())
                },
            )
            extractor = DocumentRequirementExtractor(
                selected_config, client=self.client
            )
            candidates, decisions, debug_events = extractor.run(
                chunks, progress=progress
            )
            write_jsonl(
                paths.extraction_dir / "requirements.machine.jsonl",
                (item.model_dump(mode="json") for item in candidates),
            )
            write_jsonl(
                paths.extraction_dir / "triage_decisions.jsonl",
                (item.model_dump(mode="json") for item in decisions),
            )
            write_jsonl(paths.logs_dir / "llm_debug.jsonl", debug_events)
            queue = prepare_review(paths, chunks, candidates, decisions)
            update_manifest(
                paths,
                status="awaiting_review",
                counts={
                    "machine_requirement_count": len(candidates),
                    "review_queue_count": len(queue),
                },
                artifacts={
                    "machine_requirements": str(
                        (paths.extraction_dir / "requirements.machine.jsonl").resolve()
                    ),
                    "triage_decisions": str(
                        (paths.extraction_dir / "triage_decisions.jsonl").resolve()
                    ),
                    "llm_debug": str((paths.logs_dir / "llm_debug.jsonl").resolve()),
                },
            )
            return {
                "paths": paths,
                "chunks": chunks,
                "requirements": candidates,
                "triage": decisions,
                "review_queue": queue,
                "debug_events": debug_events,
            }
        except Exception as exc:
            update_manifest(
                paths,
                status="failed",
                errors=[
                    {
                        "stage": "document_extraction",
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                ],
            )
            raise

    def create_and_extract(
        self,
        pdf_path: Path,
        output_root: Path,
        config: DocumentExtractionConfig,
        *,
        run_name: str | None = None,
        progress: ProgressCallback | None = None,
    ) -> dict[str, Any]:
        paths = self.create_run(pdf_path, output_root, config, run_name=run_name)
        return self.extract(paths, config=config, progress=progress)

    @staticmethod
    def paths(run_dir: Path) -> DocumentRunPaths:
        paths = DocumentRunPaths(run_dir.resolve())
        if not paths.manifest.exists():
            raise FileNotFoundError(f"Run manifest not found: {paths.manifest}")
        paths.ensure()
        return paths

    @staticmethod
    def load_chunks(paths: DocumentRunPaths) -> list[DocumentChunk]:
        source = paths.hitl_dir / "source_snapshot.jsonl"
        if not source.exists():
            source = paths.extraction_dir / "chunks.jsonl"
        return [DocumentChunk.model_validate(item) for item in read_jsonl(source)]

    def save_review_decision(self, paths: DocumentRunPaths, *args: Any, **kwargs: Any):
        return save_review_decision(paths, *args, **kwargs)

    def apply_review(
        self, paths: DocumentRunPaths, *, use_llm_recovery: bool = True
    ) -> dict[str, Any]:
        manifest = read_json(paths.manifest, {}) or {}
        config = DocumentExtractionConfig.model_validate(manifest.get("config", {}))
        chunks = self.load_chunks(paths)
        client = self.client or ModelClient(
            config.provider,
            config.model,
            ollama_num_ctx=config.ollama_num_ctx,
            ollama_num_predict=config.ollama_num_predict,
        )

        def recovery(chunk: DocumentChunk, selected_ids: list[str]):
            return recover_requirements_for_review(
                chunk=chunk,
                all_chunks=chunks,
                selected_evidence_span_ids=selected_ids,
                config=config,
                client=client,
            )

        return apply_review(paths, recovery=recovery if use_llm_recovery else None)

    def prepare_adjudication(self, paths: DocumentRunPaths):
        return prepare_adjudication(paths)

    def save_adjudication_decision(
        self, paths: DocumentRunPaths, *args: Any, **kwargs: Any
    ):
        return save_adjudication_decision(paths, *args, **kwargs)

    def finalize_adjudication(
        self, paths: DocumentRunPaths, require_complete: bool = True
    ):
        return finalize_adjudication(paths, require_complete=require_complete)

    def export_pipeline_input(
        self,
        paths: DocumentRunPaths,
        source: RequirementSource = "best",
        output_path: Path | None = None,
    ) -> Path:
        target = write_pipeline_input(paths, source=source, output_path=output_path)
        update_manifest(paths, artifacts={"pipeline_input": str(target.resolve())})
        return target

    def run_downstream(
        self,
        paths: DocumentRunPaths,
        *,
        source: RequirementSource = "best",
        limit: int | None = None,
        provider: str = "openai",
        model: str | None = None,
        normalization_provider: str | None = None,
        normalization_model: str | None = None,
        grounding_provider: str | None = None,
        grounding_model: str | None = None,
        grounding_think: bool = False,
        grounding_context_mode: str = "compact",
        reasoner: str = "Pellet",
        extraction_method: str = "pipeline",
        normalization_method: str = "pipeline",
        grounding_method: str = "pipeline",
    ) -> Path:
        """Run the existing main pipeline through its stable command callback."""

        input_path = self.export_pipeline_input(paths, source=source)
        output_dir = paths.pipeline_dir / "evaluation"
        update_manifest(paths, status="running_downstream_pipeline")
        try:
            from ontology_req_pipeline.cli import run_evaluation_pipeline

            run_evaluation_pipeline.callback(
                input_path=input_path,
                output_dir=output_dir,
                limit=limit,
                provider=provider,
                model=model,
                reasoner=reasoner,
                normalization_provider=normalization_provider,
                normalization_model=normalization_model,
                grounding_provider=grounding_provider,
                grounding_model=grounding_model,
                grounding_think=grounding_think,
                grounding_context_mode=grounding_context_mode,
                comparison_profile="none",
                extraction_method=extraction_method,
                normalization_method=normalization_method,
                grounding_method=grounding_method,
                raw_grounding_input=False,
            )
            update_manifest(
                paths,
                status="completed",
                artifacts={"pipeline_output_dir": str(output_dir.resolve())},
            )
            return output_dir
        except Exception as exc:
            update_manifest(
                paths,
                status="downstream_failed",
                errors=[
                    {
                        "stage": "downstream_pipeline",
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    }
                ],
            )
            raise
