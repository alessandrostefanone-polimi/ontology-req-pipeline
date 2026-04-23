# Evaluation QA Report

- Generated at (UTC): `2026-02-13T13:46:07.152889+00:00`
- Run ID: `20260213T090600.375624Z`
- Source dataset: `fsae_test_number_unit_sample.jsonl`
- Extraction provider/model: `openai` / `gpt-4o-mini`
- Normalization provider/model: `openai` / `gpt-4o-mini`
- Grounding provider/model: `openai` / `gpt-4o-mini`
- Evaluation directory: `src\ontology_req_pipeline\evaluation`

## Summary

- Records: extraction=120, normalization=120, grounding=120
- Requirements extracted: 304
- Constraints extracted: 482
- Quantity constraints extracted: 197
- Normalized quantities: 181
- Quantity coverage (normalized/extracted): 91.88%
- End-to-end success rate (vs input): 91.67%
- Invalid normalized constraint_idx count: 0
- Quantity hallucination count/rate: 0 / 0.00%
- Inferred files: 110 (unique hashes: 110, uniqueness ratio: 100.00%)
- Inferred files with requirement markers: 110/110
- Grounding failed count/rate: 10 / 8.33%

## Latency

- Run duration (s): 16801.881572600003
- Throughput (records/min): 0.4285234346456495
- Extraction p95 (s): 105.32534720000695
- Normalization p95 (s): 21.061356699996395
- Grounding p95 (s): 342.3236957999907
- Inference p95 (s): 4.460294300006353

## Status Distributions

- Extraction: `{'ok': 120}`
- Normalization: `{'ok': 120}`
- Grounding: `{'ok': 110, 'failed': 10}`
- Inference: `{'failed': 29, 'ok': 81, 'skipped': 10}`

## Robustness

- Runs in history: 2
- End-to-end success mean (history): 0.9541666666666666
- Quantity hallucination rate mean (history): 0.0
- External dependency failure rate mean (history): 0.0

