import json
from pathlib import Path
import sys

DEFAULT_ROOT = Path("../src/ontology_req_pipeline/evaluation/fsae")
CENTIM_URI = "http://qudt.org/vocab/unit/CentiM"
UNIT_PRED = "http://qudt.org/schema/qudt/unit"


def iter_idx_json_files(root: Path):
    for path in root.rglob("idx_*.json"):
        if "inferred" in path.name.lower():
            continue
        yield path


def has_centim_unit(predicted_claims):
    for claim in predicted_claims:
        if not isinstance(claim, dict):
            continue
        if claim.get("p") == UNIT_PRED and claim.get("o") == CENTIM_URI:
            return True
    return False


def main():
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ROOT
    if not root.exists():
        print(f"Root not found: {root}")
        sys.exit(1)

    results = {
        "checked": 0,
        "with_centim": [],
        "missing_centim": [],
    }

    for path in iter_idx_json_files(root):
        try:
            with path.open("r", encoding="utf-8") as f:
                obj = json.load(f)
        except Exception:
            continue

        source_text = obj.get("source_text")
        if not isinstance(source_text, str):
            continue

        if "cm" not in source_text.lower():
            continue

        results["checked"] += 1

        idx = obj.get("idx")
        if idx is None:
            name = path.name
            if name.startswith("idx_"):
                try:
                    idx = int(name.split("_")[1])
                except Exception:
                    idx = name

        predicted_claims = obj.get("predicted_claims", [])
        if has_centim_unit(predicted_claims):
            results["with_centim"].append(idx)
        else:
            results["missing_centim"].append(idx)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
