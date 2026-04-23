# Evaluation QA Report

- Generated at (UTC): `2026-04-12T00:47:26.949059+00:00`
- Run ID: `20260411T225251.953032Z`
- Source dataset: `fsae_test_number_unit_sample.jsonl`
- Extraction provider/model: `openai` / `gpt-5.1`
- Normalization provider/model: `openai` / `gpt-5.1`
- Grounding provider/model: `openai` / `gpt-5.1`
- Evaluation directory: `C:\Users\Alessandro Stefanone\repos\req-iof\src\ontology_req_pipeline\evaluation\fsae_test_pipeline`

## Summary

- Records: extraction=120, normalization=120, grounding=120
- Requirements extracted: 245
- Constraints extracted: 466
- Quantity constraints extracted: 163
- Normalized quantities: 162
- Quantity coverage (normalized/extracted): 99.39%
- End-to-end success rate (vs input): 100.00%
- Invalid normalized constraint_idx count: 0
- Quantity hallucination count/rate: 0 / 0.00%
- Inferred files: 120 (unique hashes: 120, uniqueness ratio: 100.00%)
- Inferred files with requirement markers: 120/120
- Grounding failed count/rate: 0 / 0.00%

## Latency

- Run duration (s): 6870.908965399984
- Throughput (records/min): 1.047896287995843
- Extraction p95 (s): 39.73051180000766
- Normalization p95 (s): 24.500315900018904
- Grounding p95 (s): 71.03005919998395
- Inference p95 (s): 3.397390699974494

## Status Distributions

- Extraction: `{'ok': 120}`
- Normalization: `{'ok': 120}`
- Grounding: `{'ok': 120}`
- Inference: `{'failed': 12, 'ok': 108}`

## Robustness

- Runs in history: 1
- End-to-end success mean (history): 1.0
- Quantity hallucination rate mean (history): 0.0
- External dependency failure rate mean (history): 0.0
