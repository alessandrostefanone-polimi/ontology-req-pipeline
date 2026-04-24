# Evaluation QA Report

- Generated at (UTC): `2026-04-13T18:47:39.682981+00:00`
- Run ID: `20260413T165250.439956Z`
- Source dataset: `fsae_test_number_unit_sample.jsonl`
- Extraction provider/model: `openai` / `gpt-5.1`
- Normalization provider/model: `openai` / `gpt-5.1`
- Grounding provider/model: `openai` / `gpt-5.1`
- Evaluation directory: `src\ontology_req_pipeline\evaluation\fsae_test_pipeline_rerun`

## Summary

- Records: extraction=120, normalization=120, grounding=120
- Requirements extracted: 251
- Constraints extracted: 480
- Quantity constraints extracted: 164
- Normalized quantities: 162
- Quantity coverage (normalized/extracted): 98.78%
- End-to-end success rate (vs input): 100.00%
- Invalid normalized constraint_idx count: 0
- Quantity hallucination count/rate: 0 / 0.00%
- Inferred files: 120 (unique hashes: 120, uniqueness ratio: 100.00%)
- Inferred files with requirement markers: 120/120
- Grounding failed count/rate: 0 / 0.00%

## Latency

- Run duration (s): 6885.419393799999
- Throughput (records/min): 1.045687936813735
- Extraction p95 (s): 44.568907200002286
- Normalization p95 (s): 25.60678659999394
- Grounding p95 (s): 62.0581558000049
- Inference p95 (s): 3.703959600003145

## Status Distributions

- Extraction: `{'ok': 120}`
- Normalization: `{'ok': 120}`
- Grounding: `{'ok': 120}`
- Inference: `{'ok': 109, 'failed': 11}`

## Robustness

- Runs in history: 1
- End-to-end success mean (history): 1.0
- Quantity hallucination rate mean (history): 0.0
- External dependency failure rate mean (history): 0.0
