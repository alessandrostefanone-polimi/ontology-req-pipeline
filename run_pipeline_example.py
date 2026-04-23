from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ontology_req_pipeline.extraction.llm_extractor import get_default_extractor
from ontology_req_pipeline.normalization.qudt_normalization import normalize_qudt
from ontology_req_pipeline.ontology.agentic_kg_builder import AgenticKGBuilder

INPUT_TEXT = "The valve shall have a flow rate of 100 +- 2 liters per minute."
MODEL = "gpt-5.1"
OUTPUT_ROOT = Path("pipeline_outputs")


def _model_dump_json(value: Any) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    return value


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _save_stage1_outputs(output_dir: Path, builder: AgenticKGBuilder, stage1: dict[str, Any]) -> None:
    history = stage1.get("history", [])
    _write_json(output_dir / "03_stage1_history.json", history)
    _write_text(output_dir / "03_stage1_final_owl.ttl", builder.ensure_prefixes(stage1["owl"]))

    for item in history:
        iter_idx = item.get("iter", "unknown")
        _write_text(
            output_dir / f"03_stage1_iter_{iter_idx}.ttl",
            builder.ensure_prefixes(item.get("owl", "")),
        )
        _write_text(
            output_dir / f"03_stage1_iter_{iter_idx}_pellet_report.txt",
            item.get("pellet_report", ""),
        )


def main() -> None:
    extractor = get_default_extractor()
    record = extractor.extract(
        INPUT_TEXT,
        local=False,
        idx=0,
        model=MODEL,
    )

    output_dir = OUTPUT_ROOT / f"record_{record.idx}"
    _write_json(output_dir / "01_extraction_record.json", _model_dump_json(record))

    normalized = normalize_qudt(
        idx=record.idx,
        input_text=record.original_text,
        requirements=record.requirements,
        provider="openai",
        model=MODEL,
    )
    _write_json(output_dir / "02_normalized_record.json", _model_dump_json(normalized))

    builder = AgenticKGBuilder(
        tbox_path="ontologies/Core.rdf",
        record=normalized,
        reasoner="Pellet",
        llm_provider="openai",
        llm_model=MODEL,
    )
    result = builder.two_stage_workflow()

    _save_stage1_outputs(output_dir, builder, result["stage1"])
    _write_text(output_dir / "04_initial_owl.ttl", result["initial_owl"])
    _write_json(output_dir / "05_rule_actions.json", result["rule_actions"])
    _write_text(output_dir / "06_final_owl.ttl", result["final_owl"])
    _write_json(output_dir / "07_builder_output_paths.json", result["output_paths"])

    print(f"Saved pipeline outputs to {output_dir.resolve()}")


if __name__ == "__main__":
    main()