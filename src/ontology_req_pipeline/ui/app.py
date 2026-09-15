"""Streamlit UI for PDF ingestion, HITL, and ontology-pipeline execution."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import streamlit as st

from ontology_req_pipeline.document.adapter import select_requirement_path
from ontology_req_pipeline.document.artifacts import (
    DocumentRunPaths,
    read_json,
    read_jsonl,
)
from ontology_req_pipeline.document.ingestion import render_pdf_page
from ontology_req_pipeline.document.models import DocumentExtractionConfig
from ontology_req_pipeline.document.service import DocumentPipelineService

PROJECT_ROOT = Path(__file__).resolve().parents[3]
RUNS_ROOT = PROJECT_ROOT / "artifacts" / "runs"
UPLOAD_ROOT = PROJECT_ROOT / "artifacts" / "uploads"
SERVICE = DocumentPipelineService()

st.set_page_config(
    page_title="Ontology Requirements Pipeline", page_icon="📄", layout="wide"
)


def _available_runs() -> list[Path]:
    if not RUNS_ROOT.exists():
        return []
    return sorted(
        (path.parent for path in RUNS_ROOT.glob("*/run_manifest.json")),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def _selected_paths() -> DocumentRunPaths | None:
    raw = st.session_state.get("run_dir")
    if not raw:
        return None
    try:
        return SERVICE.paths(Path(raw))
    except FileNotFoundError:
        return None


@st.cache_data(show_spinner=False)
def _render_page(pdf_path: str, page: int):
    return render_pdf_page(Path(pdf_path), page)


def _set_run(path: Path) -> None:
    st.session_state["run_dir"] = str(path.resolve())


def _download(path: Path, label: str, key: str) -> None:
    if path.exists() and path.is_file():
        st.download_button(label, path.read_bytes(), file_name=path.name, key=key)


st.title("Ontology Requirements Pipeline")
st.caption(
    "PDF ingestion → evidence-grounded extraction → human review → ontology grounding and QA"
)

with st.sidebar:
    st.subheader("Document run")
    runs = _available_runs()
    labels = [path.name for path in runs]
    current = st.session_state.get("run_dir")
    current_index = 0
    if current:
        resolved = Path(current).resolve()
        for index, run in enumerate(runs):
            if run.resolve() == resolved:
                current_index = index
                break
    if runs:
        selected_name = st.selectbox("Open run", labels, index=current_index)
        selected_run = runs[labels.index(selected_name)]
        if not current or Path(current).resolve() != selected_run.resolve():
            _set_run(selected_run)
    else:
        st.info("Create the first run from the Ingest tab.")
    active_paths = _selected_paths()
    if active_paths:
        manifest = read_json(active_paths.manifest, {}) or {}
        st.metric("Status", manifest.get("status", "unknown"))
        st.caption(active_paths.root.name)
        counts = manifest.get("counts", {})
        if counts:
            st.metric(
                "Machine requirements", counts.get("machine_requirement_count", 0)
            )
            st.metric("Review queue", counts.get("review_queue_count", 0))


ingest_tab, review_tab, adjudicate_tab, pipeline_tab, results_tab = st.tabs(
    [
        "1 · Ingest",
        "2 · Recall review",
        "3 · Adjudicate",
        "4 · Run pipeline",
        "5 · Results",
    ]
)

with ingest_tab:
    st.subheader("Create a document run")
    uploaded = st.file_uploader(
        "PDF document", type=["pdf"], accept_multiple_files=False
    )
    left, right = st.columns(2)
    with left:
        run_name = st.text_input("Run name", placeholder="optional-test-label")
        provider = st.selectbox("Document extraction provider", ["ollama", "openai"])
        default_model = "qwen3.5:9b-bf16" if provider == "ollama" else "gpt-5.1"
        model = st.text_input("Document extraction model", value=default_model)
        max_workers = st.number_input(
            "Concurrent model calls", min_value=1, max_value=32, value=4
        )
    with right:
        high_threshold = st.number_input("High-confidence threshold", 0.0, 100.0, 35.0)
        ambiguous_threshold = st.number_input("Ambiguous threshold", 0.0, 100.0, 15.0)
        bounded = st.checkbox("Bound source chunks for a smoke test", value=True)
        max_chunks = st.number_input(
            "Maximum chunks", min_value=1, value=5, disabled=not bounded
        )
        targeted_coverage = st.checkbox("Run targeted coverage audit", value=True)

    with st.expander("Model request limits"):
        limits_left, limits_right = st.columns(2)
        with limits_left:
            ollama_num_ctx = st.number_input(
                "Ollama context tokens",
                min_value=1024,
                value=8192,
                step=1024,
                disabled=provider != "ollama",
                help="Sent explicitly as num_ctx on every native Ollama document request.",
            )
            prompt_token_budget = st.number_input(
                "Maximum estimated prompt tokens",
                min_value=1,
                value=4096,
                step=256,
                help="Includes instructions and the structured-output JSON schema.",
            )
        with limits_right:
            ollama_num_predict = st.number_input(
                "Ollama maximum output tokens",
                min_value=1,
                value=3072,
                step=256,
                disabled=provider != "ollama",
                help="Sent explicitly as num_predict to bound runaway generation.",
            )
            low_batch_max_tokens = st.number_input(
                "Low-confidence batch tokens",
                min_value=1,
                value=3072,
                step=256,
                help="Token-aware limit used when grouping low-confidence chunks.",
            )

    if st.button("Ingest and extract", type="primary", disabled=uploaded is None):
        UPLOAD_ROOT.mkdir(parents=True, exist_ok=True)
        safe_name = Path(uploaded.name).name
        upload_path = UPLOAD_ROOT / f"{uuid4().hex[:10]}-{safe_name}"
        upload_path.write_bytes(uploaded.getvalue())
        config = DocumentExtractionConfig(
            provider=provider,
            model=model,
            high_threshold=high_threshold,
            ambiguous_threshold=ambiguous_threshold,
            max_workers=int(max_workers),
            ollama_num_ctx=int(ollama_num_ctx),
            ollama_num_predict=int(ollama_num_predict),
            prompt_token_budget=int(prompt_token_budget),
            low_batch_max_tokens=int(low_batch_max_tokens),
            max_chunks=int(max_chunks) if bounded else None,
            run_targeted_coverage_audit=targeted_coverage,
        )
        progress_bar = st.progress(0.0)
        status = st.status("Starting document run", expanded=True)

        def progress(stage: str, current_count: int, total: int, message: str) -> None:
            fraction = current_count / total if total else 0.0
            progress_bar.progress(
                min(1.0, max(0.0, fraction)), text=f"{stage}: {message}"
            )
            status.write(f"{stage}: {current_count}/{total} — {message}")

        try:
            result = SERVICE.create_and_extract(
                upload_path,
                RUNS_ROOT,
                config,
                run_name=run_name or None,
                progress=progress,
            )
            _set_run(result["paths"].root)
            progress_bar.progress(1.0, text="Document extraction complete")
            status.update(
                label="Extraction complete — review queue is ready", state="complete"
            )
            st.success(
                f"Extracted {len(result['requirements'])} candidates from "
                f"{len(result['chunks'])} chunks."
            )
        except Exception as exc:  # noqa: BLE001
            status.update(label="Document extraction failed", state="error")
            st.exception(exc)

    paths = _selected_paths()
    if paths:
        manifest = read_json(paths.manifest, {}) or {}
        st.divider()
        st.subheader("Active run summary")
        counts = manifest.get("counts", {})
        cols = st.columns(4)
        cols[0].metric("Chunks", counts.get("source_chunk_count", 0))
        cols[1].metric("High", counts.get("high_chunk_count", 0))
        cols[2].metric("Ambiguous", counts.get("ambiguous_chunk_count", 0))
        cols[3].metric("Low", counts.get("low_chunk_count", 0))
        st.json({"run_id": manifest.get("run_id"), "config": manifest.get("config")})

with review_tab:
    paths = _selected_paths()
    if not paths:
        st.info("Create or select a document run first.")
    else:
        queue_path = paths.hitl_dir / "review_queue.jsonl"
        queue = read_jsonl(queue_path)
        decisions_path = paths.hitl_dir / "human_decisions.jsonl"
        decisions = {item["queue_item_id"]: item for item in read_jsonl(decisions_path)}
        if not queue:
            st.info("This run does not have a review queue yet.")
        else:
            st.subheader("Recall-oriented source review")
            reviewer = st.text_input(
                "Reviewer",
                value=st.session_state.get("reviewer", ""),
                key="reviewer_input",
                placeholder="Required for saved decisions",
            )
            if reviewer.strip():
                st.session_state["reviewer"] = reviewer.strip()
            c1, c2, c3 = st.columns(3)
            priority_filter = c1.multiselect(
                "Priority", ["P0", "P1", "P2"], default=["P0", "P1", "P2"]
            )
            only_unreviewed = c2.checkbox("Only unreviewed", value=True)
            page_filter = c3.number_input("Page (0 = all)", min_value=0, value=0)
            filtered = [item for item in queue if item["priority"] in priority_filter]
            if only_unreviewed:
                filtered = [
                    item for item in filtered if item["queue_item_id"] not in decisions
                ]
            if page_filter:
                filtered = [
                    item for item in filtered if item.get("page") == page_filter
                ]

            reviewed_count = sum(
                item.get("decision") != "DEFERRED" for item in decisions.values()
            )
            st.progress(
                reviewed_count / len(queue),
                text=f"Reviewed {reviewed_count}/{len(queue)} source chunks",
            )
            if not filtered:
                st.success("No queue items match the current filters.")
            else:
                labels = {
                    item["queue_item_id"]: (
                        f"{item['priority']} · page {item.get('page') or '?'} · {item['chunk_id']} · "
                        f"{item['machine_requirement_count']} machine"
                    )
                    for item in filtered
                }
                selected_id = st.selectbox(
                    "Queue item",
                    list(labels),
                    format_func=lambda value: labels[value],
                )
                item = next(
                    row for row in filtered if row["queue_item_id"] == selected_id
                )
                current_decision = decisions.get(selected_id, {})
                source_col, review_col = st.columns([1, 1])
                with source_col:
                    st.markdown(f"**{item['reason']}**")
                    if item.get("page"):
                        try:
                            st.image(
                                _render_page(str(paths.pdf), int(item["page"])),
                                caption=f"PDF page {item['page']}",
                                use_container_width=True,
                            )
                        except Exception as exc:  # noqa: BLE001
                            st.warning(f"Page preview unavailable: {exc}")
                    with st.expander("Contextualized source text", expanded=True):
                        st.text(item["contextualized_text"])
                    if item.get("machine_requirements"):
                        st.markdown("**Machine requirements**")
                        for requirement in item["machine_requirements"]:
                            st.info(requirement.get("normalized_requirement", ""))
                with review_col:
                    st.markdown("**Evidence spans**")
                    selected_evidence: list[str] = []
                    prior_ids = set(
                        current_decision.get("selected_evidence_span_ids", [])
                    )
                    for span in item.get("evidence_spans", []):
                        checked = st.checkbox(
                            f"{span['span_id']} · {span['kind']} · {span.get('display_text') or span['text']}",
                            value=span["span_id"] in prior_ids,
                            key=f"review-span-{selected_id}-{span['span_id']}",
                        )
                        if checked:
                            selected_evidence.append(span["span_id"])
                    decision_value = st.radio(
                        "Decision",
                        ["NO_ADDITION", "ADD_REQUIREMENTS", "DEFERRED"],
                        index=["NO_ADDITION", "ADD_REQUIREMENTS", "DEFERRED"].index(
                            current_decision.get("decision", "DEFERRED")
                        ),
                        horizontal=True,
                        key=f"review-decision-{selected_id}",
                    )
                    manual_text = st.text_area(
                        "Manual requirement (optional)",
                        help="Leave empty to recover requirements with the selected evidence and configured model.",
                        key=f"manual-text-{selected_id}",
                    )
                    m1, m2 = st.columns(2)
                    manual_modality = m1.selectbox(
                        "Modality",
                        ["SHALL", "MUST", "SHOULD", "MAY", "WILL", "IS", "IMPLICIT"],
                        key=f"manual-modality-{selected_id}",
                    )
                    manual_explicitness = m2.selectbox(
                        "Explicitness",
                        ["EXPLICIT", "IMPLICIT"],
                        key=f"manual-explicit-{selected_id}",
                    )
                    notes = st.text_area(
                        "Review notes",
                        value=current_decision.get("notes", ""),
                        key=f"review-notes-{selected_id}",
                    )
                    if st.button(
                        "Save review decision",
                        type="primary",
                        key=f"save-review-{selected_id}",
                    ):
                        manual = []
                        if manual_text.strip():
                            manual.append(
                                {
                                    "literal_requirement": manual_text.strip(),
                                    "normalized_requirement": manual_text.strip(),
                                    "modality": manual_modality,
                                    "explicitness": manual_explicitness,
                                    "evidence_span_ids": selected_evidence,
                                }
                            )
                        try:
                            SERVICE.save_review_decision(
                                paths,
                                selected_id,
                                decision_value,
                                reviewer=reviewer,
                                selected_evidence_span_ids=selected_evidence,
                                manual_requirements=manual,
                                notes=notes,
                            )
                            st.success(
                                "Decision saved with an append-only audit event."
                            )
                            st.rerun()
                        except Exception as exc:  # noqa: BLE001
                            st.error(str(exc))

            st.divider()
            use_recovery = st.checkbox(
                "Use the configured model for selected-evidence recovery", value=True
            )
            if st.button("Apply saved review decisions and prepare adjudication"):
                try:
                    with st.status("Applying human decisions", expanded=True) as status:
                        result = SERVICE.apply_review(
                            paths, use_llm_recovery=use_recovery
                        )
                        status.write(
                            f"Final requirements: {len(result['requirements'])}"
                        )
                        status.write(
                            f"Recovered requirements: {len(result['recovered'])}"
                        )
                        adjudication_queue = SERVICE.prepare_adjudication(paths)
                        status.write(
                            f"Focused adjudication items: {len(adjudication_queue)}"
                        )
                        status.update(label="HITL decisions applied", state="complete")
                except Exception as exc:  # noqa: BLE001
                    st.exception(exc)

with adjudicate_tab:
    paths = _selected_paths()
    if not paths:
        st.info("Create or select a document run first.")
    else:
        queue = read_jsonl(paths.hitl_dir / "adjudication_queue.jsonl")
        saved = {
            item["adjudication_item_id"]: item
            for item in read_jsonl(paths.hitl_dir / "adjudication_decisions.jsonl")
        }
        if not queue:
            st.info(
                "Apply the recall-review decisions first. A queue appears only for HITL-affected candidates."
            )
        else:
            st.subheader("Focused requirement adjudication")
            reviewer = st.text_input(
                "Adjudicator",
                value=st.session_state.get("reviewer", ""),
                key="adjudicator_input",
            )
            only_open = st.checkbox(
                "Only unresolved candidates", value=True, key="only-open-adj"
            )
            visible = queue
            if only_open:
                visible = [
                    item
                    for item in queue
                    if saved.get(item["adjudication_item_id"], {}).get("action")
                    not in {"APPROVE", "EDIT", "REJECT"}
                ]
            resolved = len(queue) - sum(
                saved.get(item["adjudication_item_id"], {}).get("action")
                not in {"APPROVE", "EDIT", "REJECT"}
                for item in queue
            )
            st.progress(
                resolved / len(queue),
                text=f"Resolved {resolved}/{len(queue)} candidates",
            )
            if visible:
                labels = {
                    item["adjudication_item_id"]: (
                        f"{item['source_kind']} · {item.get('chunk_id')} · "
                        f"{item['requirement'].get('normalized_requirement', '')[:100]}"
                    )
                    for item in visible
                }
                selected_id = st.selectbox(
                    "Candidate",
                    list(labels),
                    format_func=lambda value: labels[value],
                    key="adj-picker",
                )
                item = next(
                    row for row in visible if row["adjudication_item_id"] == selected_id
                )
                requirement = item["requirement"]
                current = saved.get(selected_id, {})
                st.markdown(
                    f"**Source:** {item['source_kind']} · **Chunk:** {item.get('chunk_id')}"
                )
                st.write(requirement.get("literal_requirement", ""))
                action = st.radio(
                    "Action",
                    ["APPROVE", "EDIT", "REJECT", "DEFERRED"],
                    index=["APPROVE", "EDIT", "REJECT", "DEFERRED"].index(
                        current.get("action", "DEFERRED")
                    ),
                    horizontal=True,
                    key=f"adj-action-{selected_id}",
                )
                edited_text = st.text_area(
                    "Normalized requirement",
                    value=current.get("normalized_requirement")
                    or requirement.get("normalized_requirement", ""),
                    key=f"adj-text-{selected_id}",
                )
                a1, a2 = st.columns(2)
                modalities = [
                    "SHALL",
                    "MUST",
                    "SHOULD",
                    "MAY",
                    "WILL",
                    "IS",
                    "IMPLICIT",
                ]
                explicitness_values = ["EXPLICIT", "IMPLICIT"]
                current_modality = current.get("modality") or requirement.get(
                    "modality", "IMPLICIT"
                )
                current_explicit = current.get("explicitness") or requirement.get(
                    "explicitness", "IMPLICIT"
                )
                modality = a1.selectbox(
                    "Modality",
                    modalities,
                    index=modalities.index(current_modality)
                    if current_modality in modalities
                    else 6,
                    key=f"adj-modality-{selected_id}",
                )
                explicitness = a2.selectbox(
                    "Explicitness",
                    explicitness_values,
                    index=explicitness_values.index(current_explicit)
                    if current_explicit in explicitness_values
                    else 1,
                    key=f"adj-explicit-{selected_id}",
                )
                notes = st.text_area(
                    "Notes",
                    value=current.get("notes", ""),
                    key=f"adj-notes-{selected_id}",
                )
                if st.button(
                    "Save adjudication decision",
                    type="primary",
                    key=f"save-adj-{selected_id}",
                ):
                    try:
                        SERVICE.save_adjudication_decision(
                            paths,
                            selected_id,
                            action,
                            reviewer=reviewer,
                            normalized_requirement=edited_text
                            if action == "EDIT"
                            else None,
                            modality=modality if action == "EDIT" else None,
                            explicitness=explicitness if action == "EDIT" else None,
                            notes=notes,
                        )
                        st.success("Adjudication decision saved.")
                        st.rerun()
                    except Exception as exc:  # noqa: BLE001
                        st.error(str(exc))
            else:
                st.success("Every focused candidate has a terminal decision.")
            if st.button(
                "Finalize adjudicated requirements", disabled=resolved != len(queue)
            ):
                try:
                    result = SERVICE.finalize_adjudication(paths, require_complete=True)
                    st.success(
                        f"Finalized {len(result['requirements'])} adjudicated requirements."
                    )
                except Exception as exc:  # noqa: BLE001
                    st.exception(exc)

with pipeline_tab:
    paths = _selected_paths()
    if not paths:
        st.info("Create or select a document run first.")
    else:
        st.subheader("Run extraction → normalization → ontology grounding")
        available_sources = []
        for source in ("machine", "final", "adjudicated"):
            try:
                select_requirement_path(paths, source=source)
                available_sources.append(source)
            except FileNotFoundError:
                pass
        if not available_sources:
            st.info("No requirements are ready for the downstream pipeline.")
        else:
            source = st.selectbox(
                "Requirement source",
                available_sources,
                index=len(available_sources) - 1,
            )
            requirement_count = len(
                read_jsonl(select_requirement_path(paths, source=source))
            )
            st.caption(f"Selected artifact contains {requirement_count} requirements.")
            p1, p2, p3 = st.columns(3)
            with p1:
                provider = st.selectbox(
                    "Extraction provider", ["openai", "ollama"], key="pipe-provider"
                )
                model = st.text_input(
                    "Extraction model",
                    value="gpt-5.1" if provider == "openai" else "llama3.2",
                )
            with p2:
                normalization_provider = st.selectbox(
                    "Normalization provider",
                    [provider, "ollama" if provider == "openai" else "openai"],
                )
                normalization_model = st.text_input("Normalization model", value=model)
            with p3:
                grounding_provider = st.selectbox(
                    "Grounding provider",
                    [provider, "ollama" if provider == "openai" else "openai"],
                )
                grounding_model = st.text_input("Grounding model", value=model)
                reasoner = st.text_input("Reasoner", value="Pellet")
                grounding_context_mode = st.segmented_control(
                    "Ontology context",
                    options=["compact", "full"],
                    default="compact",
                    required=True,
                    format_func=lambda option: (
                        "Token-efficient" if option == "compact" else "Full ontology"
                    ),
                    key="pipe-grounding-context-mode",
                    help=(
                        "Token-efficient retrieves a small, requirement-specific IOF vocabulary signature "
                        "from Core.rdf. Full ontology embeds the complete RDF/XML in every grounding prompt."
                    ),
                    width="stretch",
                    persist_state="session",
                )
                grounding_think = False
                if grounding_provider == "ollama":
                    grounding_think = st.toggle(
                        "Enable thinking for grounding",
                        value=False,
                        key="pipe-grounding-think",
                        help=(
                            "Passes think=True to native Ollama calls made during ontology grounding "
                            "and graph repair. Extraction and normalization continue with thinking disabled."
                        ),
                    )
            bounded = st.checkbox(
                "Limit this test run", value=True, key="bounded-downstream"
            )
            limit = st.number_input(
                "Maximum requirements",
                min_value=1,
                max_value=max(1, requirement_count),
                value=min(3, max(1, requirement_count)),
                disabled=not bounded,
            )
            st.warning(
                "The downstream workflow can issue several model calls and a reasoning pass per requirement. "
                "Use a small limit for the first test."
            )
            if st.button("Run downstream pipeline", type="primary"):
                try:
                    with st.status(
                        "Running downstream pipeline", expanded=True
                    ) as status:
                        status.write("Exporting evidence-traceable pipeline input")
                        output_dir = SERVICE.run_downstream(
                            paths,
                            source=source,
                            limit=int(limit) if bounded else None,
                            provider=provider,
                            model=model,
                            normalization_provider=normalization_provider,
                            normalization_model=normalization_model,
                            grounding_provider=grounding_provider,
                            grounding_model=grounding_model,
                            grounding_think=grounding_think,
                            grounding_context_mode=grounding_context_mode,
                            reasoner=reasoner,
                        )
                        status.update(
                            label="Downstream pipeline completed", state="complete"
                        )
                    st.success(f"Outputs saved to {output_dir}")
                except Exception as exc:  # noqa: BLE001
                    st.exception(exc)

with results_tab:
    paths = _selected_paths()
    if not paths:
        st.info("Create or select a document run first.")
    else:
        manifest = read_json(paths.manifest, {}) or {}
        st.subheader("Run results and artifacts")
        st.json(
            {
                "run_id": manifest.get("run_id"),
                "status": manifest.get("status"),
                "counts": manifest.get("counts", {}),
                "errors": manifest.get("errors", []),
            }
        )
        hitl_metrics = read_json(paths.hitl_dir / "hitl_metrics.json", None)
        adjudication_metrics = read_json(
            paths.hitl_dir / "adjudication_metrics.json", None
        )
        qa_report = read_json(
            paths.pipeline_dir / "evaluation" / "qa_report.json", None
        )
        if hitl_metrics:
            st.markdown("**HITL metrics**")
            st.json(hitl_metrics)
        if adjudication_metrics:
            st.markdown("**Adjudication metrics**")
            st.json(adjudication_metrics)
        if qa_report:
            st.markdown("**Downstream QA report**")
            st.json(qa_report)

        st.markdown("**Downloads**")
        download_candidates = [
            paths.extraction_dir / "requirements.machine.jsonl",
            paths.hitl_dir / "requirements.final.jsonl",
            paths.hitl_dir / "requirements.adjudicated.jsonl",
            paths.pipeline_dir / "document_requirements.jsonl",
            paths.pipeline_dir / "evaluation" / "qa_report.md",
            paths.pipeline_dir / "evaluation" / "qa_report.json",
            paths.pipeline_dir / "evaluation" / "evaluation_report.md",
        ]
        columns = st.columns(3)
        for index, path in enumerate(download_candidates):
            if path.exists():
                with columns[index % 3]:
                    _download(
                        path, f"Download {path.name}", f"download-{index}-{path.name}"
                    )
        ttl_files = sorted((paths.pipeline_dir / "evaluation").glob("*.ttl"))
        if ttl_files:
            st.markdown("**Knowledge graph artifacts**")
            selected_ttl = st.selectbox(
                "TTL artifact", ttl_files, format_func=lambda path: path.name
            )
            st.code(
                selected_ttl.read_text(encoding="utf-8", errors="replace")[:20000],
                language="turtle",
            )
            _download(selected_ttl, "Download selected TTL", "download-selected-ttl")
