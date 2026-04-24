import json
import sys
from pathlib import Path
from typing import List, Optional, Callable, Any

from ollama import Client
from pydantic import BaseModel


# --- Pydantic models for the two-pass extraction ---

class Structure(BaseModel):
    system_agent: Optional[str]
    modal_verb: Optional[str]
    condition: Optional[str]
    system_response: Optional[str]


class ReqStructure(BaseModel):
    req_idx: int
    structure: Structure
    references: List[str] = []


class StructurePass(BaseModel):
    idx: int
    original_text: str
    requirements: List[ReqStructure]


class ConstraintNormalization(BaseModel):
    operator: Optional[str]
    primary_value: Optional[float]
    secondary_value: Optional[float]
    tolerance: Optional[float]
    unit: Optional[str]


class Constraint(BaseModel):
    constraint_idx: int
    text_span: str
    target_attribute: str
    normalization: ConstraintNormalization


class ReqConstraints(BaseModel):
    req_idx: int
    constraints: List[Constraint]
    references: List[str] = []


class ConstraintPass(BaseModel):
    idx: int
    original_text: str
    requirements: List[ReqConstraints]


# --- Prompts (matching the notebook) ---

structure_prompt = """You are an expert in requirements structuring.
Given a single requirement text, split it into atomic requirements (req_idx starting at 0, preserve order).
For each atomic requirement, extract:
- system_agent: entity the requirement applies to (no articles; null if missing)
- modal_verb: modal verb surface form (must/shall/should/etc.; null if absent)
- condition: explicit when/if/during clause; null if none
- system_response: the required behavior/state without the modal verb
Return JSON ONLY, conforming to StructurePass.
Do NOT include constraints here.

Schema (StructurePass):
{
  "idx": int,
  "original_text": string,
  "requirements": [
    {
      "req_idx": int,
      "structure": {
        "system_agent": string | null,
        "modal_verb": string | null,
        "condition": string | null,
        "system_response": string | null
      },
      "references": []
    }
  ]
}

Input idx: {idx}
Input text:
{original_text}
"""


constraint_prompt = """You are extracting constraints for already-structured requirements.
You are given the original text and a list of requirement structures (req_idx aligned to the text).
For EACH requirement, find all constraint spans (quantitative or categorical), keep constraint_idx sequential across the document starting at 0, and fill:
- text_span: exact snippet from original_text
- target_attribute: concise attribute name (reuse dataset style)
- normalization: operator (Range, <, <=, >, >=, ==, or null), primary_value, secondary_value, tolerance, unit; use null where missing.
Leave structure unchanged; references stays [].

Output JSON ONLY conforming to ConstraintPass:
{
  "idx": int,
  "original_text": string,
  "requirements": [
    {
      "req_idx": int,
      "constraints": [
        {
          "constraint_idx": int,
          "text_span": string,
          "target_attribute": string,
          "normalization": {
            "operator": string | null,
            "primary_value": number | null,
            "secondary_value": number | null,
            "tolerance": number | null,
            "unit": string | null
          }
        }
      ],
      "references": []
    }
  ]
}

Original text:
{original_text}

Requirement structures (JSON):
{structures_json}
"""


# --- Ollama client ---
client = Client(timeout=300)


def _call_with_retry(
    build_messages: Callable[[Optional[str]], List[dict]],
    schema: Any,
    max_attempts: int = 2,
) -> Any:
    """
    Invoke Ollama with a single retry. On the second attempt, the previous error
    string is fed back to the model.
    """
    last_err: Optional[str] = None
    for attempt in range(max_attempts):
        messages = build_messages(last_err)
        try:
            response = client.chat(
                model="qwen3:4b",
                messages=messages,
                format=schema.model_json_schema(),
            )
            return schema.model_validate_json(response["message"]["content"])
        except Exception as exc:
            last_err = str(exc)
            if attempt + 1 >= max_attempts:
                raise
    raise RuntimeError("Unreachable")


def run_structure_pass(idx: int, original_text: str) -> StructurePass:
    def build_messages(error_text: Optional[str]) -> List[dict]:
        user_content = structure_prompt.replace("{idx}", str(idx)).replace("{original_text}", original_text)
        if error_text:
            user_content += (
                "\nThe previous attempt failed with the following error:\n"
                f"{error_text}\nPlease return ONLY valid JSON conforming to StructurePass."
            )
        return [
            {"role": "system", "content": "Extract requirement structures as JSON."},
            {"role": "user", "content": user_content},
        ]

    return _call_with_retry(build_messages, StructurePass)


def run_constraint_pass(structured: StructurePass) -> ConstraintPass:
    def build_messages(error_text: Optional[str]) -> List[dict]:
        structures_json = json.dumps(structured.model_dump(), ensure_ascii=False)
        user_content = constraint_prompt.replace("{original_text}", structured.original_text).replace("{structures_json}", structures_json)
        if error_text:
            user_content += (
                "\nThe previous attempt failed with the following error:\n"
                f"{error_text}\nPlease return ONLY valid JSON conforming to ConstraintPass."
            )
        return [
            {"role": "system", "content": "Extract constraints aligned to req_idx."},
            {"role": "user", "content": user_content},
        ]

    constraints = _call_with_retry(build_messages, ConstraintPass)
    # Ensure sequential constraint_idx across document
    next_idx = 0
    for req in constraints.requirements:
        for c in req.constraints:
            c.constraint_idx = next_idx
            next_idx += 1
    return constraints


def merge_passes(struct_pass: StructurePass, constr_pass: ConstraintPass) -> List[dict]:
    merged: List[dict] = []
    for req_struct in struct_pass.requirements:
        req_constr = next(
            rc for rc in constr_pass.requirements if rc.req_idx == req_struct.req_idx
        )
        merged.append(
            {
                "req_idx": req_struct.req_idx,
                "structure": req_struct.structure.model_dump(),
                "constraints": [c.model_dump() for c in req_constr.constraints],
                "references": req_constr.references,
            }
        )
    return merged


def print_progress(done: int, total: int) -> None:
    width = 30
    filled = int(width * done / total) if total else width
    bar = "█" * filled + "-" * (width - filled)
    percent = (done / total * 100) if total else 100
    sys.stdout.write(f"\r[{bar}] {done}/{total} ({percent:0.1f}%)")
    sys.stdout.flush()


def main() -> None:
    input_path = Path("datasets/extraction/requirements_dataset.jsonl")
    output_path = Path("datasets/extraction/requirements_dataset_labeled.jsonl")
    flush_every = 25

    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Count total lines for progress
    with input_path.open("r", encoding="utf-8") as f_in:
        total = sum(1 for _ in f_in)

    buffer: List[dict] = []
    processed = 0

    with input_path.open("r", encoding="utf-8") as f_in, output_path.open(
        "w", encoding="utf-8"
    ) as f_out:
        for line in f_in:
            processed += 1
            record = json.loads(line)
            idx = record.get("idx", processed - 1)
            text = record.get("original_text", "")

            try:
                struct_pass = run_structure_pass(idx, text)
                constr_pass = run_constraint_pass(struct_pass)
                merged_requirements = merge_passes(struct_pass, constr_pass)
                output_record = {
                    **record,
                    "idx": idx,
                    "original_text": text,
                    "requirements": merged_requirements,
                }
            except Exception as exc:
                # Keep minimal record of the failure and continue
                output_record = {
                    "idx": idx,
                    "original_text": text,
                    "error": str(exc),
                }

            buffer.append(output_record)

            if len(buffer) >= flush_every:
                for item in buffer:
                    f_out.write(json.dumps(item, ensure_ascii=False) + "\n")
                f_out.flush()
                buffer.clear()

            print_progress(processed, total)

        # Flush remaining
        if buffer:
            for item in buffer:
                f_out.write(json.dumps(item, ensure_ascii=False) + "\n")
            f_out.flush()

    sys.stdout.write("\nDone. Results written to ")
    sys.stdout.write(str(output_path))
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
