# Evaluation QA Report

- Generated at (UTC): `2026-02-12T18:20:58.810807+00:00`
- Run ID: `20260212T162403.485830Z`
- Source dataset: `fsae_test_number_unit_sample.jsonl`
- Extraction provider/model: `openai` / `gpt-5.1`
- Normalization provider/model: `openai` / `gpt-5.1`
- Grounding provider/model: `openai` / `gpt-5.1`
- Evaluation directory: `src\ontology_req_pipeline\evaluation`

## Summary

- Records: extraction=120, normalization=120, grounding=120
- Requirements extracted: 245
- Constraints extracted: 480
- Quantity constraints extracted: 164
- Normalized quantities: 161
- Quantity coverage (normalized/extracted): 98.17%
- End-to-end success rate (vs input): 99.17%
- Invalid normalized constraint_idx count: 0
- Quantity hallucination count/rate: 0 / 0.00%
- Inferred files: 119 (unique hashes: 119, uniqueness ratio: 100.00%)
- Inferred files with requirement markers: 119/119
- Grounding failed count/rate: 1 / 0.83%

## Latency

- Run duration (s): 7009.5525978000005
- Throughput (records/min): 1.0271696944338178
- Extraction p95 (s): 42.482047099998454
- Normalization p95 (s): 21.282352499998524
- Grounding p95 (s): 74.37804080000205
- Inference p95 (s): 4.324713700003485

## Status Distributions

- Extraction: `{'ok': 120}`
- Normalization: `{'ok': 120}`
- Grounding: `{'ok': 119, 'failed': 1}`
- Inference: `{'ok': 108, 'failed': 11, 'skipped': 1}`

## Robustness

- Runs in history: 1
- End-to-end success mean (history): 0.9916666666666667
- Quantity hallucination rate mean (history): 0.0
- External dependency failure rate mean (history): 0.0

