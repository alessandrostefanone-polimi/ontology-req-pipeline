# Evaluation QA Report

- Generated at (UTC): `2026-04-13T10:22:18.319460+00:00`
- Run ID: `20260413T101912.660513Z`
- Source dataset: `techreq_no_fsae.jsonl`
- Extraction provider/model: `openai` / `gpt-5.4`
- Normalization provider/model: `openai` / `gpt-5.4`
- Grounding provider/model: `openai` / `gpt-5.4`
- Evaluation directory: `C:\Users\Alessandro Stefanone\repos\req-iof\src\ontology_req_pipeline\evaluation\techreq_no_fsae_comparison\grounding-focused\raw_requirement__zero_shot_llm_grounding`

## Summary

- Records: extraction=20, normalization=20, grounding=20
- Requirements extracted: 0
- Constraints extracted: 0
- Quantity constraints extracted: 0
- Normalized quantities: 0
- Quantity coverage (normalized/extracted): n/a
- End-to-end success rate (vs input): 100.00%
- Invalid normalized constraint_idx count: 0
- Quantity hallucination count/rate: 19 / 95.00%
- Inferred files: 20 (unique hashes: 20, uniqueness ratio: 100.00%)
- Inferred files with requirement markers: 20/20
- Grounding failed count/rate: 0 / 0.00%

## Latency

- Run duration (s): 184.28413720000026
- Throughput (records/min): 6.511683632854745
- Extraction p95 (s): 0.0
- Normalization p95 (s): 0.0
- Grounding p95 (s): 11.621252000000823
- Inference p95 (s): 3.4075477000005776

## Status Distributions

- Extraction: `{'skipped': 20}`
- Normalization: `{'skipped': 20}`
- Grounding: `{'ok': 20}`
- Inference: `{'failed': 9, 'ok': 11}`

## Robustness

- Runs in history: 1
- End-to-end success mean (history): 1.0
- Quantity hallucination rate mean (history): 0.95
- External dependency failure rate mean (history): 0.0
