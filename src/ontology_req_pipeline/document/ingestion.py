"""Docling-backed PDF conversion and contextual chunk generation."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from ontology_req_pipeline.document.models import DocumentChunk, SourceLocator

ProgressCallback = Callable[[str, int, int, str], None]


def _as_jsonable(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    if isinstance(value, Mapping):
        return {str(key): _as_jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_as_jsonable(item) for item in value]
    return value


def _find_page(metadata: Mapping[str, Any]) -> int | None:
    for item in metadata.get("doc_items", []) or []:
        if not isinstance(item, Mapping):
            continue
        for provenance in item.get("prov", []) or []:
            if isinstance(provenance, Mapping) and provenance.get("page_no") is not None:
                try:
                    return int(provenance["page_no"])
                except (TypeError, ValueError):
                    return None
    return None


def _headings(metadata: Mapping[str, Any]) -> list[str]:
    values = metadata.get("headings", []) or []
    if isinstance(values, str):
        return [values]
    return [str(value) for value in values if value not in (None, "")]


def ingest_pdf(
    pdf_path: Path,
    *,
    table_cell_matching: bool = True,
    max_chunks: int | None = None,
    progress: ProgressCallback | None = None,
) -> list[DocumentChunk]:
    """Convert a PDF with Docling and return table-aware contextual chunks."""

    try:
        from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
        from docling.chunking import HybridChunker
        from docling.datamodel.base_models import InputFormat
        from docling.datamodel.pipeline_options import (
            PdfPipelineOptions,
            TableFormerMode,
        )
        from docling.document_converter import DocumentConverter, PdfFormatOption
        from docling_core.transforms.chunker.hierarchical_chunker import (
            ChunkingDocSerializer,
            ChunkingSerializerProvider,
        )
        from docling_core.transforms.serializer.markdown import MarkdownTableSerializer
    except ImportError as exc:  # pragma: no cover - depends on optional installation
        raise RuntimeError(
            "PDF ingestion requires the document dependencies. Install with "
            "`pip install -e .[document]`."
        ) from exc

    source = pdf_path.resolve()
    if not source.exists():
        raise FileNotFoundError(f"PDF not found: {source}")
    if progress:
        progress("ingestion", 0, 1, "Converting PDF with Docling")

    pdf_options = PdfPipelineOptions(do_table_structure=True)
    # Docling's default docling-parse backend can terminate the entire Python
    # process on some otherwise valid PDFs (rather than raising an exception).
    # PDFium is already a document/UI dependency and fails safely in Python.
    layout_options = getattr(pdf_options, "layout_options", None)
    layout_engine = getattr(layout_options, "engine_options", None)
    if hasattr(layout_engine, "compile_model"):
        # Docling enables torch.compile() by default, but its CPU path requires
        # an external C++ compiler which is not present in a normal Windows venv.
        layout_engine.compile_model = False
    pdf_options.table_structure_options.mode = TableFormerMode.ACCURATE
    pdf_options.table_structure_options.do_cell_matching = table_cell_matching
    converter = DocumentConverter(
        format_options={
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=pdf_options,
                backend=PyPdfiumDocumentBackend,
            )
        }
    )
    document = converter.convert(source).document

    class MarkdownTableSerializerProvider(ChunkingSerializerProvider):
        def get_serializer(self, doc):  # type: ignore[no-untyped-def]
            return ChunkingDocSerializer(doc=doc, table_serializer=MarkdownTableSerializer())

    chunker = HybridChunker(serializer_provider=MarkdownTableSerializerProvider())
    docling_chunks = list(chunker.chunk(document))
    if max_chunks is not None:
        docling_chunks = docling_chunks[:max_chunks]

    results: list[DocumentChunk] = []
    total = len(docling_chunks)
    for index, chunk in enumerate(docling_chunks):
        record = _as_jsonable(chunk)
        if not isinstance(record, dict):
            record = {}
        raw_text = str(record.get("text") or record.get("content") or "")
        contextualized = str(chunker.contextualize(chunk=chunk) or raw_text)
        metadata = record.get("meta") or record.get("metadata") or {}
        if not isinstance(metadata, dict):
            metadata = {}
        headings = _headings(metadata)
        chunk_id = f"chunk-{index}"
        results.append(
            DocumentChunk(
                chunk_id=chunk_id,
                chunk_index=index,
                raw_text=raw_text,
                contextualized_text=contextualized,
                headings=headings,
                metadata=metadata,
                source_locator=SourceLocator(
                    chunk_id=chunk_id,
                    chunk_index=index,
                    page=_find_page(metadata),
                    section=" > ".join(headings) or None,
                ),
            )
        )
        if progress:
            progress("chunking", index + 1, total, f"Prepared {chunk_id}")
    return results


def render_pdf_page(pdf_path: Path, page_number: int, scale: float = 1.5):
    """Render a one-based PDF page for the UI using Docling's PDFium dependency."""

    try:
        import pypdfium2 as pdfium
    except ImportError as exc:  # pragma: no cover - optional UI path
        raise RuntimeError("PDF page preview requires pypdfium2") from exc
    pdf = pdfium.PdfDocument(str(pdf_path))
    if page_number < 1 or page_number > len(pdf):
        raise IndexError(f"Page {page_number} is outside 1..{len(pdf)}")
    page = pdf[page_number - 1]
    return page.render(scale=scale).to_pil()
