import json
from pathlib import Path
import sys

DEFAULT_ROOT = Path("../src/ontology_req_pipeline/evaluation/fsae")


def iter_idx_json_files(root: Path):
    for path in root.rglob("idx_*.json"):
        if "inferred" in path.name.lower():
            continue
        yield path


def main():
    root = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ROOT
    if not root.exists():
        print(f"Root not found: {root}")
        sys.exit(1)

    matches = []
    for path in iter_idx_json_files(root):
        try:
            with path.open("r", encoding="utf-8") as f:
                obj = json.load(f)
        except Exception:
            continue

        source_text = obj.get("source_text")
        if not isinstance(source_text, str):
            continue

        if "cm" in source_text.lower():
            idx = obj.get("idx")
            if idx is None:
                # fallback to filename if idx missing
                name = path.name
                if name.startswith("idx_"):
                    try:
                        idx = int(name.split("_")[1])
                    except Exception:
                        idx = name
            matches.append(idx)

    for idx in matches:
        print(idx)


if __name__ == "__main__":
    main()
