from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import random
import re
from typing import Dict, Iterable, List, Tuple


NUM_UNIT_RE = re.compile(
    r"(?i)(\d+(?:\.\d+)?)\s*(?:\\mathrm\{([A-Za-z%µμ]{1,16})\}|([\\]?[A-Za-z%µμ]{1,16}(?:/[A-Za-z%µμ]{1,16})?))"
)

UNIT_ALIASES = {
    r"\%": "%",
    "pct": "%",
    "percent": "%",
    "khz": "hz",
    "mhz": "hz",
    "ghz": "hz",
    "ma": "a",
    "ua": "a",
    "kv": "v",
    "mv": "v",
    "kw": "w",
    "mw": "w",
    "kpa": "pa",
    "mpa": "pa",
    "mm": "m",
    "cm": "m",
    "um": "m",
    "µm": "m",
    "km": "m",
    "ms": "s",
    "us": "s",
    "ns": "s",
    "hr": "s",
}

KNOWN_UNIT_ROOTS = {
    "%",
    "hz",
    "a",
    "v",
    "w",
    "n",
    "nm",
    "pa",
    "bar",
    "psi",
    "gauss",
    "t",
    "m",
    "kg",
    "g",
    "mg",
    "s",
    "min",
    "rpm",
    "rad",
    "ohm",
    "j",
    "wh",
    "ah",
}

ELECTRICAL_UNITS = {"a", "v", "w", "hz", "ohm", "wh", "ah"}
TIME_UNITS = {"s", "min", "rpm"}
MECHANICAL_UNITS = {"m", "kg", "g", "mg", "n", "nm", "pa", "bar", "psi", "gauss", "t", "rad", "j"}


def _normalize_unit_token(token: str) -> str:
    cleaned = token.strip().replace("μ", "µ").lower()
    cleaned = cleaned.strip("{}()")
    cleaned = UNIT_ALIASES.get(cleaned, cleaned)
    return cleaned


def extract_unit_tokens(text: str) -> List[str]:
    tokens: List[str] = []
    for match in NUM_UNIT_RE.finditer(text or ""):
        raw = (match.group(2) or match.group(3) or "").strip()
        if not raw:
            continue
        token = _normalize_unit_token(raw)
        tokens.append(token)
    return tokens


def has_number_plus_unit(text: str) -> bool:
    for token in extract_unit_tokens(text):
        if token in KNOWN_UNIT_ROOTS:
            return True
        if "/" in token:
            parts = [part for part in re.split(r"[/-]", token) if part]
            if parts and all(part in KNOWN_UNIT_ROOTS for part in parts):
                return True
    return False


def classify_unit_family(tokens: Iterable[str]) -> str:
    token_set = set(tokens)
    if "%" in token_set:
        return "percent"
    if token_set & ELECTRICAL_UNITS:
        return "electrical_frequency"
    if token_set & TIME_UNITS:
        return "time_rate"
    if token_set & MECHANICAL_UNITS:
        return "mechanical_physical"
    return "other"


def percentile(values: List[int], p: float) -> int:
    if not values:
        return 0
    sorted_values = sorted(values)
    rank = max(0, min(len(sorted_values) - 1, math.ceil(p * len(sorted_values)) - 1))
    return int(sorted_values[rank])


def assign_length_bucket(length: int, p33: int, p66: int) -> str:
    if length <= p33:
        return "short"
    if length <= p66:
        return "medium"
    return "long"


def _allocate_proportional(total_n: int, counts: Dict[str, int]) -> Dict[str, int]:
    if total_n <= 0:
        return {k: 0 for k in counts}
    total = sum(counts.values())
    if total == 0:
        return {k: 0 for k in counts}
    if total_n >= total:
        return dict(counts)

    exact = {k: (total_n * v / total) for k, v in counts.items()}
    alloc = {k: min(counts[k], int(math.floor(exact[k]))) for k in counts}
    remaining = total_n - sum(alloc.values())
    order = sorted(counts, key=lambda k: (-(exact[k] - math.floor(exact[k])), k))

    for key in order:
        if remaining <= 0:
            break
        if alloc[key] < counts[key]:
            alloc[key] += 1
            remaining -= 1
    return alloc


def stratified_sample(records: List[Dict], sample_size: int, seed: int) -> List[Dict]:
    if sample_size >= len(records):
        return list(records)

    by_stratum: Dict[str, List[Dict]] = defaultdict(list)
    for record in records:
        by_stratum[record["stratum"]].append(record)

    rng = random.Random(seed)
    for values in by_stratum.values():
        rng.shuffle(values)

    counts = {k: len(v) for k, v in by_stratum.items()}
    alloc = _allocate_proportional(sample_size, counts)

    selected: List[Dict] = []
    leftovers: List[Dict] = []
    for stratum in sorted(by_stratum):
        take = alloc[stratum]
        bucket = by_stratum[stratum]
        selected.extend(bucket[:take])
        leftovers.extend(bucket[take:])

    if len(selected) < sample_size:
        rng.shuffle(leftovers)
        need = sample_size - len(selected)
        selected.extend(leftovers[:need])

    return sorted(selected, key=lambda r: int(r["row"].get("idx", -1)))


def summarize_strata(records: List[Dict]) -> Dict[str, int]:
    counter = Counter(r["stratum"] for r in records)
    return dict(sorted(counter.items()))


def enforce_family_coverage(
    selected: List[Dict],
    pool: List[Dict],
    families: List[str],
    seed: int,
) -> Tuple[List[Dict], List[Dict]]:
    """Ensure each family is represented at least once in selected, if available in pool."""
    rng = random.Random(seed)
    selected_local = list(selected)
    pool_local = list(pool)

    def family_counts(items: List[Dict]) -> Counter:
        return Counter(item["family"] for item in items)

    counts = family_counts(selected_local)
    for family in families:
        if counts.get(family, 0) > 0:
            continue
        candidates = [item for item in pool_local if item["family"] == family]
        if not candidates:
            continue

        # Replace one sample from the most over-represented family (count > 1).
        replaceable_families = [(fam, cnt) for fam, cnt in counts.items() if cnt > 1]
        if not replaceable_families:
            continue
        replace_family = sorted(replaceable_families, key=lambda x: (-x[1], x[0]))[0][0]

        incoming = candidates[0]
        outgoing_indices = [i for i, item in enumerate(selected_local) if item["family"] == replace_family]
        if not outgoing_indices:
            continue
        outgoing_index = rng.choice(outgoing_indices)
        outgoing = selected_local[outgoing_index]

        selected_local[outgoing_index] = incoming
        pool_local.remove(incoming)
        pool_local.append(outgoing)
        counts = family_counts(selected_local)

    selected_local = sorted(selected_local, key=lambda r: int(r["row"].get("idx", -1)))
    pool_local = sorted(pool_local, key=lambda r: int(r["row"].get("idx", -1)))
    return selected_local, pool_local


def write_jsonl(path: Path, rows: List[Dict]) -> None:
    with path.open("w", encoding="utf-8") as out:
        for row in rows:
            out.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Create reproducible FSAE number+unit samples.")
    parser.add_argument("--input", type=Path, default=Path("artifacts/private_inputs/fsae.jsonl"))
    parser.add_argument("--dev-size", type=int, default=20)
    parser.add_argument("--test-size", type=int, default=120)
    parser.add_argument("--seed", type=int, default=20260212)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("datasets"),
    )
    args = parser.parse_args()

    rows = [json.loads(line) for line in args.input.read_text(encoding="utf-8").splitlines() if line.strip()]
    enriched: List[Dict] = []
    for row in rows:
        text = str(row.get("original_text", ""))
        tokens = extract_unit_tokens(text)
        if not has_number_plus_unit(text):
            continue
        enriched.append(
            {
                "row": row,
                "tokens": tokens,
                "family": classify_unit_family(tokens),
                "length": len(text),
            }
        )

    if not enriched:
        raise RuntimeError("No eligible rows with number+unit patterns found.")

    p33 = percentile([r["length"] for r in enriched], 0.33)
    p66 = percentile([r["length"] for r in enriched], 0.66)

    for record in enriched:
        record["length_bucket"] = assign_length_bucket(record["length"], p33, p66)
        record["stratum"] = f"{record['family']}|{record['length_bucket']}"

    total_needed = args.dev_size + args.test_size
    if total_needed > len(enriched):
        raise RuntimeError(
            f"Requested {total_needed} rows but only {len(enriched)} eligible rows are available."
        )

    dev_records = stratified_sample(enriched, args.dev_size, args.seed)
    dev_idx = {int(r["row"].get("idx", -1)) for r in dev_records}
    remaining = [r for r in enriched if int(r["row"].get("idx", -1)) not in dev_idx]
    families = sorted({r["family"] for r in enriched})
    dev_records, remaining = enforce_family_coverage(
        selected=dev_records,
        pool=remaining,
        families=families,
        seed=args.seed + 17,
    )

    test_records = stratified_sample(remaining, args.test_size, args.seed + 1)
    selected_test_idx = {int(r["row"].get("idx", -1)) for r in test_records}
    test_pool = [r for r in remaining if int(r["row"].get("idx", -1)) not in selected_test_idx]
    test_records, _ = enforce_family_coverage(
        selected=test_records,
        pool=test_pool,
        families=families,
        seed=args.seed + 23,
    )

    args.output_dir.mkdir(parents=True, exist_ok=True)
    dev_path = args.output_dir / "fsae_dev_number_unit_sample.jsonl"
    test_path = args.output_dir / "fsae_test_number_unit_sample.jsonl"
    manifest_path = args.output_dir / "fsae_number_unit_sampling_manifest.json"

    write_jsonl(dev_path, [r["row"] for r in dev_records])
    write_jsonl(test_path, [r["row"] for r in test_records])

    manifest = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_file": str(args.input.as_posix()),
        "seed": args.seed,
        "filter_rule": "Rows containing number+unit patterns (including LaTeX \\\\mathrm{...} forms).",
        "requested_sizes": {"dev": args.dev_size, "test": args.test_size},
        "actual_sizes": {"dev": len(dev_records), "test": len(test_records)},
        "eligible_rows": len(enriched),
        "total_rows_in_source": len(rows),
        "length_bucket_thresholds": {"p33": p33, "p66": p66},
        "strata_counts_eligible": summarize_strata(enriched),
        "strata_counts_dev": summarize_strata(dev_records),
        "strata_counts_test": summarize_strata(test_records),
        "dev_indices": [int(r["row"].get("idx", -1)) for r in dev_records],
        "test_indices": [int(r["row"].get("idx", -1)) for r in test_records],
        "output_files": {
            "dev_jsonl": str(dev_path.as_posix()),
            "test_jsonl": str(test_path.as_posix()),
            "manifest_json": str(manifest_path.as_posix()),
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"Eligible rows: {len(enriched)} / {len(rows)}")
    print(f"Dev sample: {len(dev_records)} -> {dev_path}")
    print(f"Test sample: {len(test_records)} -> {test_path}")
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
