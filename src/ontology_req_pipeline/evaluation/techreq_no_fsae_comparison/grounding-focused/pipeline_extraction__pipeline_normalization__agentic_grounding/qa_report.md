# Evaluation QA Report

- Generated at (UTC): `2026-04-13T10:00:26.053164+00:00`
- Run ID: `20260413T094656.027811Z`
- Source dataset: `techreq_no_fsae.jsonl`
- Extraction provider/model: `openai` / `gpt-5.4`
- Normalization provider/model: `openai` / `gpt-5.4`
- Grounding provider/model: `openai` / `gpt-5.4`
- Evaluation directory: `C:\Users\Alessandro Stefanone\repos\req-iof\src\ontology_req_pipeline\evaluation\techreq_no_fsae_comparison\grounding-focused\pipeline_extraction__pipeline_normalization__agentic_grounding`

## Summary

- Records: extraction=20, normalization=20, grounding=20
- Requirements extracted: 21
- Constraints extracted: 36
- Quantity constraints extracted: 26
- Normalized quantities: 26
- Quantity coverage (normalized/extracted): 100.00%
- End-to-end success rate (vs input): 100.00%
- Invalid normalized constraint_idx count: 0
- Quantity hallucination count/rate: 0 / 0.00%
- Inferred files: 20 (unique hashes: 20, uniqueness ratio: 100.00%)
- Inferred files with requirement markers: 20/20
- Grounding failed count/rate: 0 / 0.00%

## Latency

- Run duration (s): 809.1662102999999
- Throughput (records/min): 1.4830080454732506
- Extraction p95 (s): 16.931883199999902
- Normalization p95 (s): 19.824365399999806
- Grounding p95 (s): 16.71874099999968
- Inference p95 (s): 3.6995796999999584

## Status Distributions

- Extraction: `{'ok': 20}`
- Normalization: `{'ok': 20}`
- Grounding: `{'ok': 20}`
- Inference: `{'failed': 1, 'ok': 19}`

## Robustness

- Runs in history: 1
- End-to-end success mean (history): 1.0
- Quantity hallucination rate mean (history): 0.0
- External dependency failure rate mean (history): 0.0
