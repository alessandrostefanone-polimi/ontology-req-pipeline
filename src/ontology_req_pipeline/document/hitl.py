"""Persistent recall review and focused adjudication for document requirements."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import uuid4

from ontology_req_pipeline.document.artifacts import (
    DocumentRunPaths,
    append_jsonl,
    read_json,
    read_jsonl,
    stable_hash,
    update_manifest,
    write_json,
    write_jsonl,
)
from ontology_req_pipeline.document.models import (
    AdjudicationDecision,
    DocumentChunk,
    RequirementCandidate,
    RequirementDecision,
    ReviewDecision,
    ReviewQueueItem,
)

RecoveryCallback = Callable[[DocumentChunk, list[str]], list[RequirementCandidate]]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _candidate_fingerprint(item: dict[str, Any]) -> str:
    existing = str(item.get("requirement_fingerprint", ""))
    if existing:
        return existing
    value = "|".join(
        [
            str(item.get("target_chunk_id", "")),
            ",".join(sorted(item.get("evidence_span_ids", []))),
            " ".join(str(item.get("normalized_requirement", "")).lower().split()),
        ]
    )
    return "REQFP-" + sha256(value.encode("utf-8")).hexdigest()[:16]


def _deduplicate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        item = dict(row)
        fingerprint = _candidate_fingerprint(item)
        item["requirement_fingerprint"] = fingerprint
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        results.append(item)
    return results


def _hitl_paths(paths: DocumentRunPaths) -> dict[str, Path]:
    root = paths.hitl_dir
    return {
        "source_snapshot": root / "source_snapshot.jsonl",
        "machine": root / "requirements.machine.jsonl",
        "review_queue": root / "review_queue.jsonl",
        "decision_template": root / "human_decisions.template.jsonl",
        "decisions": root / "human_decisions.jsonl",
        "review_events": root / "human_review_events.jsonl",
        "recovered": root / "requirements.hitl_recovered.jsonl",
        "final": root / "requirements.final.jsonl",
        "lineage": root / "requirement_lineage.jsonl",
        "coverage": root / "source_coverage_status.jsonl",
        "metrics": root / "hitl_metrics.json",
        "application_summary": root / "hitl_application_summary.json",
        "adjudication_queue": root / "adjudication_queue.jsonl",
        "adjudication_decisions": root / "adjudication_decisions.jsonl",
        "adjudication_events": root / "adjudication_events.jsonl",
        "adjudicated": root / "requirements.adjudicated.jsonl",
        "adjudication_lineage": root / "adjudication_lineage.jsonl",
        "adjudication_metrics": root / "adjudication_metrics.json",
        "adjudication_progress": root / "adjudication_progress.json",
    }


def prepare_review(
    paths: DocumentRunPaths,
    chunks: list[DocumentChunk],
    candidates: list[RequirementCandidate],
    triage: list[RequirementDecision],
) -> list[ReviewQueueItem]:
    manifest = read_json(paths.manifest, {}) or {}
    run_id = str(manifest["run_id"])
    document_sha = str(manifest["document_sha256"])
    hp = _hitl_paths(paths)
    machine_rows = []
    by_chunk: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for candidate in candidates:
        row = candidate.model_dump(mode="json")
        row["requirement_origin"] = "MACHINE_BASELINE"
        row["baseline_run_id"] = run_id
        row["requirement_fingerprint"] = _candidate_fingerprint(row)
        machine_rows.append(row)
        by_chunk[candidate.target_chunk_id].append(row)

    triage_by_chunk = {item.chunk_id: item for item in triage}
    queue: list[ReviewQueueItem] = []
    for chunk in chunks:
        machine = by_chunk.get(chunk.chunk_id, [])
        decision = triage_by_chunk.get(chunk.chunk_id)
        if not machine:
            priority = "P0"
            reason = "No machine requirement was extracted from this source chunk."
        elif decision and decision.status != "ok":
            priority = "P0"
            reason = f"Triage requires attention: {decision.status}."
        elif chunk.requirement_route in {"AMBIGUOUS", "LOW_CONFIDENCE"}:
            priority = "P1"
            reason = f"Machine output came through the {chunk.requirement_route.lower()} route."
        else:
            priority = "P2"
            reason = "High-confidence chunk with machine-extracted requirements."
        queue_item_id = (
            f"HITL-{document_sha[:10]}-{chunk.chunk_id}-"
            f"{stable_hash(chunk.raw_text + chunk.chunk_id, 10)}"
        )
        queue.append(
            ReviewQueueItem(
                queue_item_id=queue_item_id,
                run_id=run_id,
                document_sha256=document_sha,
                chunk_id=chunk.chunk_id,
                chunk_index=chunk.chunk_index,
                page=chunk.source_locator.page,
                priority=priority,
                reason=reason,
                requirement_score=chunk.requirement_score,
                requirement_route=chunk.requirement_route,
                machine_requirement_count=len(machine),
                machine_requirements=machine,
                evidence_spans=chunk.evidence_spans,
                raw_text=chunk.raw_text,
                contextualized_text=chunk.contextualized_text,
            )
        )

    queue.sort(key=lambda item: ({"P0": 0, "P1": 1, "P2": 2}[item.priority], item.chunk_index))
    write_jsonl(hp["source_snapshot"], (item.model_dump(mode="json") for item in chunks))
    write_jsonl(hp["machine"], machine_rows)
    write_jsonl(hp["review_queue"], (item.model_dump(mode="json") for item in queue))
    write_jsonl(
        hp["decision_template"],
        (
            {
                "queue_item_id": item.queue_item_id,
                "decision": "DEFERRED",
                "reviewer": "",
                "selected_evidence_span_ids": [],
                "manual_requirements": [],
                "notes": "",
            }
            for item in queue
        ),
    )
    update_manifest(
        paths,
        status="awaiting_review",
        counts={
            "machine_requirement_count": len(machine_rows),
            "review_queue_count": len(queue),
            "review_p0_count": sum(item.priority == "P0" for item in queue),
            "review_p1_count": sum(item.priority == "P1" for item in queue),
            "review_p2_count": sum(item.priority == "P2" for item in queue),
        },
        artifacts={key: str(value.resolve()) for key, value in hp.items() if value.exists()},
    )
    return queue


def save_review_decision(
    paths: DocumentRunPaths,
    queue_item_id: str,
    decision: str,
    *,
    reviewer: str,
    selected_evidence_span_ids: list[str] | None = None,
    manual_requirements: list[dict[str, Any]] | None = None,
    notes: str = "",
) -> ReviewDecision:
    reviewer = reviewer.strip()
    if not reviewer:
        raise ValueError("reviewer is required")
    hp = _hitl_paths(paths)
    queue_by_id = {item["queue_item_id"]: item for item in read_jsonl(hp["review_queue"])}
    if queue_item_id not in queue_by_id:
        raise KeyError(f"Unknown review queue item: {queue_item_id}")
    queue_item = queue_by_id[queue_item_id]
    selected = list(dict.fromkeys(selected_evidence_span_ids or []))
    valid_ids = {item["span_id"] for item in queue_item.get("evidence_spans", [])}
    invalid = [span_id for span_id in selected if span_id not in valid_ids]
    if invalid:
        raise ValueError(f"Unknown evidence span IDs: {invalid}")
    manifest = read_json(paths.manifest, {}) or {}
    record = ReviewDecision(
        decision_id="HITLDEC-" + uuid4().hex,
        queue_item_id=queue_item_id,
        run_id=str(manifest["run_id"]),
        document_sha256=str(manifest["document_sha256"]),
        decision=decision,
        reviewer=reviewer,
        selected_evidence_span_ids=selected,
        manual_requirements=manual_requirements or [],
        notes=notes,
    )
    current = {item["queue_item_id"]: item for item in read_jsonl(hp["decisions"])}
    current[queue_item_id] = record.model_dump(mode="json")
    position = {item["queue_item_id"]: index for index, item in enumerate(read_jsonl(hp["review_queue"]))}
    write_jsonl(hp["decisions"], sorted(current.values(), key=lambda item: position[item["queue_item_id"]]))
    append_jsonl(hp["review_events"], {"event_type": "REVIEW_DECISION_SAVED", **record.model_dump(mode="json")})
    return record


def _manual_candidate(
    queue_item: dict[str, Any],
    decision: dict[str, Any],
    payload: dict[str, Any],
) -> dict[str, Any]:
    selected_ids = payload.get("evidence_span_ids") or decision.get("selected_evidence_span_ids") or []
    evidence_by_id = {item["span_id"]: item for item in queue_item.get("evidence_spans", [])}
    if not selected_ids or any(span_id not in evidence_by_id for span_id in selected_ids):
        raise ValueError("Manual requirement must use valid evidence_span_ids")
    normalized = str(payload.get("normalized_requirement") or payload.get("text") or "").strip()
    literal = str(payload.get("literal_requirement") or normalized).strip()
    if not normalized:
        raise ValueError("Manual requirement needs normalized_requirement")
    row = {
        "evidence_span_ids": selected_ids,
        "literal_requirement": literal,
        "normalized_requirement": normalized,
        "modality": str(payload.get("modality", "IMPLICIT")).upper(),
        "explicitness": str(payload.get("explicitness", "IMPLICIT")).upper(),
        "context_additions": payload.get("context_additions", []),
        "explanation": payload.get("explanation", "Requirement entered by a human reviewer."),
        "evidence_spans": [evidence_by_id[span_id] for span_id in selected_ids],
        "target_chunk_id": queue_item["chunk_id"],
        "source_chunk_ids": [queue_item["chunk_id"]],
        "route": "HUMAN_REVIEW",
        "extraction_pass": "HITL_MANUAL",
        "grounding_status": "HUMAN_REVIEWED",
        "normalization_status": "HUMAN_REVIEWED",
        "requirement_origin": "HITL_MANUAL",
        "human_decision_id": decision.get("decision_id"),
        "human_queue_item_id": decision.get("queue_item_id"),
        "human_reviewer": decision.get("reviewer"),
        "human_reviewed_at": decision.get("reviewed_at"),
        "human_review_notes": decision.get("notes", ""),
        "source": {
            "target_chunk_id": queue_item["chunk_id"],
            "source_locator": {
                "chunk_id": queue_item["chunk_id"],
                "chunk_index": queue_item["chunk_index"],
                "page": queue_item.get("page"),
            },
            "raw_text": queue_item.get("raw_text", ""),
        },
    }
    row["requirement_fingerprint"] = _candidate_fingerprint(row)
    return row


def apply_review(
    paths: DocumentRunPaths,
    *,
    recovery: RecoveryCallback | None = None,
) -> dict[str, Any]:
    hp = _hitl_paths(paths)
    manifest = read_json(paths.manifest, {}) or {}
    queue = read_jsonl(hp["review_queue"])
    queue_by_id = {item["queue_item_id"]: item for item in queue}
    chunks_by_id = {
        item["chunk_id"]: DocumentChunk.model_validate(item) for item in read_jsonl(hp["source_snapshot"])
    }
    decisions = read_jsonl(hp["decisions"])
    valid_decisions: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    recovered: list[dict[str, Any]] = []
    application_run_id = "HITLAPPLY-" + uuid4().hex

    for decision in decisions:
        if decision.get("document_sha256") != manifest.get("document_sha256"):
            errors.append({"decision_id": decision.get("decision_id"), "error": "STALE_DOCUMENT_HASH"})
            continue
        if not str(decision.get("reviewer", "")).strip():
            errors.append({"decision_id": decision.get("decision_id"), "error": "MISSING_REVIEWER"})
            continue
        queue_item = queue_by_id.get(decision.get("queue_item_id"))
        if queue_item is None:
            errors.append({"decision_id": decision.get("decision_id"), "error": "UNKNOWN_QUEUE_ITEM"})
            continue
        valid_decisions.append(decision)
        if decision.get("decision") != "ADD_REQUIREMENTS":
            continue
        for manual in decision.get("manual_requirements", []) or []:
            try:
                recovered.append(_manual_candidate(queue_item, decision, manual))
            except Exception as exc:  # noqa: BLE001
                errors.append({"decision_id": decision.get("decision_id"), "error": str(exc)})
        selected_ids = decision.get("selected_evidence_span_ids", []) or []
        if recovery and selected_ids:
            try:
                items = recovery(chunks_by_id[queue_item["chunk_id"]], selected_ids)
                for candidate in items:
                    row = candidate.model_dump(mode="json")
                    row["requirement_origin"] = "HITL_LLM_RECOVERY"
                    row["human_decision_id"] = decision["decision_id"]
                    row["human_queue_item_id"] = decision["queue_item_id"]
                    row["human_reviewer"] = decision["reviewer"]
                    row["human_reviewed_at"] = decision["reviewed_at"]
                    row["human_review_notes"] = decision.get("notes", "")
                    row["hitl_application_run_id"] = application_run_id
                    recovered.append(row)
            except Exception as exc:  # noqa: BLE001
                errors.append({"decision_id": decision.get("decision_id"), "error": str(exc)})

    machine = read_jsonl(hp["machine"])
    recovered = _deduplicate(recovered)
    machine_fingerprints = {_candidate_fingerprint(item) for item in machine}
    final = _deduplicate(machine + recovered)
    recovered = [item for item in final if _candidate_fingerprint(item) not in machine_fingerprints]
    write_jsonl(hp["recovered"], recovered)
    write_jsonl(hp["final"], final)
    write_jsonl(
        hp["lineage"],
        (
            {
                "requirement_fingerprint": _candidate_fingerprint(item),
                "requirement_origin": item.get("requirement_origin"),
                "target_chunk_id": item.get("target_chunk_id"),
                "human_decision_id": item.get("human_decision_id"),
                "human_reviewer": item.get("human_reviewer"),
                "application_run_id": application_run_id,
            }
            for item in final
        ),
    )

    decisions_by_queue = {item["queue_item_id"]: item for item in valid_decisions}
    recovered_by_chunk: dict[str, int] = Counter(item.get("target_chunk_id") for item in recovered)
    coverage = []
    for item in sorted(queue, key=lambda row: row["chunk_index"]):
        decision = decisions_by_queue.get(item["queue_item_id"])
        value = decision.get("decision") if decision else "UNREVIEWED"
        coverage.append(
            {
                "run_id": manifest["run_id"],
                "chunk_id": item["chunk_id"],
                "queue_item_id": item["queue_item_id"],
                "human_decision": value,
                "reviewer": decision.get("reviewer") if decision else None,
                "hitl_recovered_requirement_count": recovered_by_chunk.get(item["chunk_id"], 0),
                "coverage_status": "DEFERRED" if value == "DEFERRED" else "REVIEWED" if decision else "UNREVIEWED",
            }
        )
    write_jsonl(hp["coverage"], coverage)
    reviewed = [item for item in valid_decisions if item.get("decision") != "DEFERRED"]
    metrics = {
        "run_id": manifest["run_id"],
        "application_run_id": application_run_id,
        "calculated_at": _now(),
        "machine_requirement_count": len(machine),
        "hitl_recovered_requirement_count": len(recovered),
        "final_requirement_count": len(final),
        "review_queue_count": len(queue),
        "reviewed_queue_count": len(reviewed),
        "review_coverage": len(reviewed) / len(queue) if queue else 1.0,
        "full_source_review_complete": len(reviewed) == len(queue),
        "application_error_count": len(errors),
    }
    write_json(hp["metrics"], metrics)
    write_json(
        hp["application_summary"],
        {"valid_decisions": valid_decisions, "application_errors": errors, "metrics": metrics},
    )
    update_manifest(
        paths,
        status="review_applied",
        counts={
            "hitl_recovered_requirement_count": len(recovered),
            "hitl_final_requirement_count": len(final),
            "reviewed_queue_count": len(reviewed),
        },
        artifacts={key: str(value.resolve()) for key, value in hp.items() if value.exists()},
        errors=errors,
    )
    return {"requirements": final, "recovered": recovered, "metrics": metrics, "errors": errors}


def prepare_adjudication(paths: DocumentRunPaths) -> list[dict[str, Any]]:
    hp = _hitl_paths(paths)
    final = read_jsonl(hp["final"]) or read_jsonl(hp["machine"])
    recovered = read_jsonl(hp["recovered"])
    recovered_fingerprints = {_candidate_fingerprint(item) for item in recovered}
    add_chunk_ids = {
        item["queue_item_id"]: item
        for item in read_jsonl(hp["decisions"])
        if item.get("decision") == "ADD_REQUIREMENTS"
    }
    queue_by_id = {item["queue_item_id"]: item for item in read_jsonl(hp["review_queue"])}
    affected_chunks = {queue_by_id[key]["chunk_id"] for key in add_chunk_ids if key in queue_by_id}
    queue: list[dict[str, Any]] = []
    manifest = read_json(paths.manifest, {}) or {}
    for item in final:
        fingerprint = _candidate_fingerprint(item)
        chunk_id = item.get("target_chunk_id")
        if fingerprint not in recovered_fingerprints and chunk_id not in affected_chunks:
            continue
        source_kind = "HITL_RECOVERED" if fingerprint in recovered_fingerprints else "MACHINE_IN_ADD_CHUNK"
        queue.append(
            {
                "adjudication_item_id": "ADJ-" + stable_hash(fingerprint + source_kind, 20),
                "run_id": manifest["run_id"],
                "document_sha256": manifest["document_sha256"],
                "source_kind": source_kind,
                "chunk_id": chunk_id,
                "requirement_fingerprint": fingerprint,
                "requirement": item,
            }
        )
    queue.sort(key=lambda item: (str(item.get("chunk_id")), item["source_kind"], item["requirement_fingerprint"]))
    write_jsonl(hp["adjudication_queue"], queue)
    update_manifest(
        paths,
        status="awaiting_adjudication" if queue else "adjudication_not_required",
        counts={"adjudication_queue_count": len(queue)},
        artifacts={"adjudication_queue": str(hp["adjudication_queue"].resolve())},
    )
    return queue


def save_adjudication_decision(
    paths: DocumentRunPaths,
    adjudication_item_id: str,
    action: str,
    *,
    reviewer: str,
    normalized_requirement: str | None = None,
    modality: str | None = None,
    explicitness: str | None = None,
    evidence_span_ids: list[str] | None = None,
    notes: str = "",
) -> AdjudicationDecision:
    reviewer = reviewer.strip()
    if not reviewer:
        raise ValueError("reviewer is required")
    hp = _hitl_paths(paths)
    queue = read_jsonl(hp["adjudication_queue"])
    position = {item["adjudication_item_id"]: index for index, item in enumerate(queue)}
    if adjudication_item_id not in position:
        raise KeyError(f"Unknown adjudication item: {adjudication_item_id}")
    record = AdjudicationDecision(
        adjudication_decision_id="ADJDEC-" + uuid4().hex,
        adjudication_item_id=adjudication_item_id,
        action=action,
        reviewer=reviewer,
        normalized_requirement=normalized_requirement,
        modality=modality,
        explicitness=explicitness,
        evidence_span_ids=evidence_span_ids,
        notes=notes,
    )
    current = {
        item["adjudication_item_id"]: item for item in read_jsonl(hp["adjudication_decisions"])
    }
    current[adjudication_item_id] = record.model_dump(mode="json")
    write_jsonl(
        hp["adjudication_decisions"],
        sorted(current.values(), key=lambda item: position[item["adjudication_item_id"]]),
    )
    append_jsonl(
        hp["adjudication_events"],
        {"event_type": "REQUIREMENT_ADJUDICATED", **record.model_dump(mode="json")},
    )
    return record


def finalize_adjudication(paths: DocumentRunPaths, require_complete: bool = True) -> dict[str, Any]:
    hp = _hitl_paths(paths)
    queue = read_jsonl(hp["adjudication_queue"])
    decisions = {
        item["adjudication_item_id"]: item for item in read_jsonl(hp["adjudication_decisions"])
    }
    unresolved = [
        item["adjudication_item_id"]
        for item in queue
        if decisions.get(item["adjudication_item_id"], {}).get("action")
        not in {"APPROVE", "EDIT", "REJECT"}
    ]
    progress = {
        "calculated_at": _now(),
        "focused_candidate_count": len(queue),
        "resolved_candidate_count": len(queue) - len(unresolved),
        "unresolved_items": unresolved,
    }
    write_json(hp["adjudication_progress"], progress)
    if require_complete and unresolved:
        raise ValueError(f"Focused review has {len(unresolved)} unresolved candidates")

    final_input = read_jsonl(hp["final"]) or read_jsonl(hp["machine"])
    queue_by_fingerprint = {item["requirement_fingerprint"]: item for item in queue}
    output: list[dict[str, Any]] = []
    lineage: list[dict[str, Any]] = []
    action_counts: Counter[str] = Counter()
    for original in final_input:
        fingerprint = _candidate_fingerprint(original)
        queue_item = queue_by_fingerprint.get(fingerprint)
        if queue_item is None:
            output.append(original)
            lineage.append({"original_requirement_fingerprint": fingerprint, "action": "UNAFFECTED"})
            continue
        decision = decisions.get(queue_item["adjudication_item_id"])
        action = decision.get("action") if decision else "DEFERRED"
        action_counts[action] += 1
        if action == "REJECT":
            lineage.append({"original_requirement_fingerprint": fingerprint, "action": action, "included": False})
            continue
        revised = dict(original)
        if action == "EDIT" and decision:
            if decision.get("normalized_requirement"):
                revised["normalized_requirement"] = decision["normalized_requirement"].strip()
            if decision.get("modality"):
                revised["modality"] = decision["modality"]
            if decision.get("explicitness"):
                revised["explicitness"] = decision["explicitness"]
            if decision.get("evidence_span_ids") is not None:
                revised["evidence_span_ids"] = decision["evidence_span_ids"]
            revised["normalization_status"] = "human_edited"
            revised["requirement_origin"] = (
                "HITL_EDITED_RECOVERY"
                if queue_item["source_kind"] == "HITL_RECOVERED"
                else "HITL_EDITED_MACHINE"
            )
            revised.pop("requirement_fingerprint", None)
        if decision:
            revised["adjudication_decision_id"] = decision["adjudication_decision_id"]
            revised["adjudication_reviewer"] = decision["reviewer"]
            revised["adjudication_action"] = action
        revised["requirement_fingerprint"] = _candidate_fingerprint(revised)
        output.append(revised)
        lineage.append(
            {
                "original_requirement_fingerprint": fingerprint,
                "output_requirement_fingerprint": revised["requirement_fingerprint"],
                "action": action,
                "included": True,
            }
        )
    output = _deduplicate(output)
    write_jsonl(hp["adjudicated"], output)
    write_jsonl(hp["adjudication_lineage"], lineage)
    metrics = {
        **progress,
        "finalized_at": _now(),
        "pre_adjudication_requirement_count": len(final_input),
        "adjudicated_requirement_count": len(output),
        "action_count_by_type": dict(action_counts),
        "focused_review_coverage": (len(queue) - len(unresolved)) / len(queue) if queue else 1.0,
    }
    write_json(hp["adjudication_metrics"], metrics)
    update_manifest(
        paths,
        status="adjudicated",
        counts={"adjudicated_requirement_count": len(output)},
        artifacts={
            "requirements_adjudicated": str(hp["adjudicated"].resolve()),
            "adjudication_metrics": str(hp["adjudication_metrics"].resolve()),
        },
    )
    append_jsonl(
        hp["adjudication_events"],
        {"event_type": "ADJUDICATION_FINALIZED", "finalized_at": metrics["finalized_at"], **metrics},
    )
    return {"requirements": output, "metrics": metrics, "unresolved": unresolved}
