import json
import os
from pathlib import Path
from typing import Dict, List, Set, Tuple

BASE = Path("../src/ontology_req_pipeline/evaluation/fsae")
MODEL_DIRS = {
    "gpt-4o-mini": BASE / "gpt-4o-mini",
    "gpt-5.1": BASE / "gpt-5.1",
}

def load_grounding_status(path: Path) -> Dict[int, str]:
    status = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            idx = int(obj["idx"])
            status[idx] = obj.get("inference_status")
    return status

def extract_idx_from_filename(name: str) -> int:
    # idx_00080_00000.json -> 80
    base = os.path.basename(name)
    if not base.startswith("idx_"):
        raise ValueError(f"Unexpected filename: {name}")
    parts = base.split("_")
    return int(parts[1])

def load_predicted_claims(path: Path) -> List[dict]:
    with path.open("r", encoding="utf-8") as f:
        obj = json.load(f)
    return obj.get("predicted_claims", [])

def claim_to_key(claim: dict) -> Tuple[str, str, str]:
    # Only compare s/p/o, ignoring supported_by_sentence and notes
    return (claim.get("s"), claim.get("p"), claim.get("o"))

def claim_key_to_obj(key: Tuple[str, str, str]) -> dict:
    return {"s": key[0], "p": key[1], "o": key[2]}

def collect_claims(model_dir: Path, idx: int) -> Tuple[Set[Tuple[str, str, str]], List[Path]]:
    gt_dir = model_dir / "annotation" / "ground_truth_claims"
    if not gt_dir.exists():
        return set(), []
    files = sorted(p for p in gt_dir.glob("idx_*.json") if "inferred" not in p.name.lower())
    matched = [p for p in files if extract_idx_from_filename(p.name) == idx]
    all_keys: Set[Tuple[str, str, str]] = set()
    for p in matched:
        for claim in load_predicted_claims(p):
            all_keys.add(claim_to_key(claim))
    return all_keys, matched

def main():
    grounding = {
        model: load_grounding_status(path / "grounding.jsonl")
        for model, path in MODEL_DIRS.items()
    }

    all_indices = set()
    for m in MODEL_DIRS:
        all_indices.update(grounding[m].keys())

    report = {
        "models": list(MODEL_DIRS.keys()),
        "results": [],
    }

    for idx in sorted(all_indices):
        status_a = grounding["gpt-4o-mini"].get(idx)
        status_b = grounding["gpt-5.1"].get(idx)
        both_failed = (status_a == "failed" and status_b == "failed")

        # Only compare claims if both inference_status are ok
        compare = (status_a == "ok" and status_b == "ok")

        entry = {
            "idx": idx,
            "inference_status": {
                "gpt-4o-mini": status_a,
                "gpt-5.1": status_b,
            },
            "both_failed": both_failed,
        }

        if compare:
            claims_a, files_a = collect_claims(MODEL_DIRS["gpt-4o-mini"], idx)
            claims_b, files_b = collect_claims(MODEL_DIRS["gpt-5.1"], idx)

            common = claims_a & claims_b
            only_a = claims_a - claims_b
            only_b = claims_b - claims_a

            entry.update({
                "files": {
                    "gpt-4o-mini": [str(p) for p in files_a],
                    "gpt-5.1": [str(p) for p in files_b],
                },
                "counts": {
                    "gpt-4o-mini": len(claims_a),
                    "gpt-5.1": len(claims_b),
                    "common": len(common),
                    "gpt-4o-mini_minus_gpt-5.1": len(only_a),
                    "gpt-5.1_minus_gpt-4o-mini": len(only_b),
                },
                "differences": {
                    "only_in_gpt-4o-mini": [claim_key_to_obj(k) for k in sorted(only_a)],
                    "only_in_gpt-5.1": [claim_key_to_obj(k) for k in sorted(only_b)],
                }
            })
        else:
            entry["reason_skipped"] = "inference_status_not_ok_for_both"

        report["results"].append(entry)

    output_path = BASE / "ground_truth_claims_comparison.json"
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print(f"Wrote {output_path}")

if __name__ == "__main__":
    print("Started...")
    main()
    print("Done!")
