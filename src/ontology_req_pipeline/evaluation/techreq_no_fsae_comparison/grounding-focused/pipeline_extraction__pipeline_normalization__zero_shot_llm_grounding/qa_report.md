# Evaluation QA Report

- Generated at (UTC): `2026-04-13T10:11:27.070493+00:00`
- Run ID: `20260413T100027.414832Z`
- Source dataset: `techreq_no_fsae.jsonl`
- Extraction provider/model: `openai` / `gpt-5.4`
- Normalization provider/model: `openai` / `gpt-5.4`
- Grounding provider/model: `openai` / `gpt-5.4`
- Evaluation directory: `C:\Users\Alessandro Stefanone\repos\req-iof\src\ontology_req_pipeline\evaluation\techreq_no_fsae_comparison\grounding-focused\pipeline_extraction__pipeline_normalization__zero_shot_llm_grounding`

## Summary

- Records: extraction=20, normalization=20, grounding=20
- Requirements extracted: 21
- Constraints extracted: 39
- Quantity constraints extracted: 27
- Normalized quantities: 26
- Quantity coverage (normalized/extracted): 96.30%
- End-to-end success rate (vs input): 100.00%
- Invalid normalized constraint_idx count: 0
- Quantity hallucination count/rate: 0 / 0.00%
- Inferred files: 20 (unique hashes: 20, uniqueness ratio: 100.00%)
- Inferred files with requirement markers: 20/20
- Grounding failed count/rate: 0 / 0.00%

## Latency

- Run duration (s): 658.6019273000002
- Throughput (records/min): 1.822041433919745
- Extraction p95 (s): 20.184989600000335
- Normalization p95 (s): 18.691882299999634
- Grounding p95 (s): 14.323508000000402
- Inference p95 (s): 3.719753800000035

## Status Distributions

- Extraction: `{'ok': 20}`
- Normalization: `{'ok': 20}`
- Grounding: `{'ok': 20}`
- Inference: `{'ok': 12, 'failed': 8}`

## Robustness

- Runs in history: 1
- End-to-end success mean (history): 1.0
- Quantity hallucination rate mean (history): 0.0
- External dependency failure rate mean (history): 0.0
