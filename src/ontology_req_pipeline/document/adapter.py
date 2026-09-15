"""Convert evidence-rich document requirements into main-pipeline JSONL rows."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from ontology_req_pipeline.document.artifacts import (
    DocumentRunPaths,
    read_json,
    read_jsonl,
    write_jsonl,
)

RequirementSource = Literal["machine", "final", "adjudicated", "best"]


def select_requirement_path(paths: DocumentRunPaths, source: RequirementSource = "best") -> Path:
    candidates = {
        "machine": paths.hitl_dir / "requirements.machine.jsonl",
        "final": paths.hitl_dir / "requirements.final.jsonl",
        "adjudicated": paths.hitl_dir / "requirements.adjudicated.jsonl",
    }
    if source != "best":
        selected = candidates[source]
        if not selected.exists():
            raise FileNotFoundError(f"Requirement source is not available: {selected}")
        return selected
    for name in ("adjudicated", "final", "machine"):
        if candidates[name].exists():
            return candidates[name]
    raise FileNotFoundError(f"No requirement artifacts found in {paths.hitl_dir}")


def to_pipeline_rows(paths: DocumentRunPaths, source: RequirementSource = "best") -> list[dict[str, Any]]:
    manifest = read_json(paths.manifest, {}) or {}
    requirements_path = select_requirement_path(paths, source)
    rows: list[dict[str, Any]] = []
    for idx, requirement in enumerate(read_jsonl(requirements_path)):
        text = str(
            requirement.get("normalized_requirement")
            or requirement.get("text")
            or requirement.get("literal_requirement")
            or ""
        ).strip()
        if not text:
            continue
        locator = requirement.get("source", {}).get("source_locator", {}) or {}
        fingerprint = requirement.get("requirement_fingerprint")
        rows.append(
            {
                "idx": idx,
                "original_text": text,
                "source": {
                    "doc_id": manifest.get("document_sha256"),
                    "section": locator.get("section"),
                    "sentence_id": fingerprint,
                    "page": locator.get("page"),
                    "chunk_id": locator.get("chunk_id") or requirement.get("target_chunk_id"),
                    "chunk_index": locator.get("chunk_index"),
                    "requirement_fingerprint": fingerprint,
                    "evidence_span_ids": requirement.get("evidence_span_ids", []),
                    "requirement_origin": requirement.get("requirement_origin"),
                    "reviewer": requirement.get("adjudication_reviewer")
                    or requirement.get("human_reviewer"),
                },
                "document_requirement": {
                    "literal_requirement": requirement.get("literal_requirement"),
                    "source_quotes": requirement.get("source", {}).get("source_quotes", []),
                    "explicitness": requirement.get("explicitness"),
                    "modality": requirement.get("modality"),
                    "requirement_fingerprint": fingerprint,
                },
            }
        )
    return rows


def write_pipeline_input(
    paths: DocumentRunPaths,
    source: RequirementSource = "best",
    output_path: Path | None = None,
) -> Path:
    target = output_path or (paths.pipeline_dir / "document_requirements.jsonl")
    write_jsonl(target, to_pipeline_rows(paths, source))
    return target
