# Evaluation QA Report

- Generated at (UTC): `2026-04-13T10:19:11.045412+00:00`
- Run ID: `20260413T101128.431105Z`
- Source dataset: `techreq_no_fsae.jsonl`
- Extraction provider/model: `openai` / `gpt-5.4`
- Normalization provider/model: `openai` / `gpt-5.4`
- Grounding provider/model: `openai` / `gpt-5.4`
- Evaluation directory: `src\ontology_req_pipeline\evaluation\techreq_no_fsae_comparison\grounding-focused\raw_requirement__agentic_grounding`

## Summary

- Records: extraction=20, normalization=20, grounding=20
- Requirements extracted: 0
- Constraints extracted: 0
- Quantity constraints extracted: 0
- Normalized quantities: 0
- Quantity coverage (normalized/extracted): n/a
- End-to-end success rate (vs input): 95.00%
- Invalid normalized constraint_idx count: 0
- Quantity hallucination count/rate: 19 / 100.00%
- Inferred files: 19 (unique hashes: 19, uniqueness ratio: 100.00%)
- Inferred files with requirement markers: 19/19
- Grounding failed count/rate: 1 / 5.00%

## Latency

- Run duration (s): 461.56438570000137
- Throughput (records/min): 2.5998539687590903
- Extraction p95 (s): 0.0
- Normalization p95 (s): 0.0
- Grounding p95 (s): 37.164603300001545
- Inference p95 (s): 3.6661285999998654

## Status Distributions

- Extraction: `{'skipped': 20}`
- Normalization: `{'skipped': 20}`
- Grounding: `{'ok': 19, 'failed': 1}`
- Inference: `{'ok': 19, 'skipped': 1}`

## Robustness

- Runs in history: 1
- End-to-end success mean (history): 0.95
- Quantity hallucination rate mean (history): 1.0
- External dependency failure rate mean (history): 0.0
