"""Durable, run-scoped artifact storage for document workflows."""

from __future__ import annotations

import json
import shutil
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import uuid4

from ontology_req_pipeline.document.models import DocumentExtractionConfig, RunManifest


def sha256_file(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def stable_hash(value: str, length: int = 16) -> str:
    return sha256(value.encode("utf-8")).hexdigest()[:length]


def read_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    temp_path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temp_path.replace(path)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as stream:
        for line_no, raw in enumerate(stream, start=1):
            if not raw.strip():
                continue
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise TypeError(f"Expected object at {path}:{line_no}")
            rows.append(value)
    return rows


def write_jsonl(path: Path, values: Iterable[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    with temp_path.open("w", encoding="utf-8") as stream:
        for value in values:
            stream.write(json.dumps(dict(value), ensure_ascii=False) + "\n")
    temp_path.replace(path)


def append_jsonl(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(dict(value), ensure_ascii=False) + "\n")


@dataclass(frozen=True)
class DocumentRunPaths:
    root: Path

    @property
    def source_dir(self) -> Path:
        return self.root / "source"

    @property
    def extraction_dir(self) -> Path:
        return self.root / "document_extraction"

    @property
    def hitl_dir(self) -> Path:
        return self.root / "hitl"

    @property
    def pipeline_dir(self) -> Path:
        return self.root / "pipeline"

    @property
    def logs_dir(self) -> Path:
        return self.root / "logs"

    @property
    def manifest(self) -> Path:
        return self.root / "run_manifest.json"

    @property
    def pdf(self) -> Path:
        manifest = read_json(self.manifest, {}) or {}
        stored = manifest.get("stored_pdf_path")
        if stored:
            return Path(stored)
        matches = list(self.source_dir.glob("*.pdf"))
        if not matches:
            raise FileNotFoundError(f"No PDF found in {self.source_dir}")
        return matches[0]

    def ensure(self) -> None:
        for path in (self.source_dir, self.extraction_dir, self.hitl_dir, self.pipeline_dir, self.logs_dir):
            path.mkdir(parents=True, exist_ok=True)


def create_run(
    pdf_path: Path,
    output_root: Path,
    config: DocumentExtractionConfig,
    run_name: str | None = None,
) -> DocumentRunPaths:
    source = pdf_path.resolve()
    if not source.exists() or not source.is_file():
        raise FileNotFoundError(f"PDF not found: {source}")
    if source.suffix.lower() != ".pdf":
        raise ValueError(f"Expected a PDF file: {source}")

    document_sha = sha256_file(source)
    safe_name = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in (run_name or "")).strip("-")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = safe_name or f"doc-{stamp}-{document_sha[:8]}"
    root = output_root.resolve() / run_id
    if root.exists():
        root = output_root.resolve() / f"{run_id}-{uuid4().hex[:6]}"
        run_id = root.name
    paths = DocumentRunPaths(root)
    paths.ensure()
    stored_pdf = paths.source_dir / source.name
    shutil.copy2(source, stored_pdf)
    manifest = RunManifest.new(
        run_id=run_id,
        document_sha256=document_sha,
        source_path=source,
        stored_pdf_path=stored_pdf,
        output_dir=root,
        config=config,
    )
    write_json(paths.manifest, manifest.model_dump(mode="json"))
    return paths


def update_manifest(paths: DocumentRunPaths, **updates: Any) -> dict[str, Any]:
    manifest = read_json(paths.manifest, {}) or {}
    for key, value in updates.items():
        if key in {"counts", "artifacts"}:
            merged = dict(manifest.get(key, {}))
            merged.update(value)
            manifest[key] = merged
        elif key == "errors":
            manifest.setdefault("errors", []).extend(value)
        else:
            manifest[key] = value
    manifest["updated_at"] = datetime.now(timezone.utc).isoformat()
    write_json(paths.manifest, manifest)
    return manifest
