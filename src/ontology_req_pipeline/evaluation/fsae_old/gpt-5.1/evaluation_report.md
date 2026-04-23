# Evaluation Report

- Generated at (UTC): `2026-02-14T11:18:16.203778+00:00`
- Evaluation directory: `src\ontology_req_pipeline\evaluation\fsae\gpt-5.1`
- Run ID: `20260212T162403.485830Z`
- Dataset: `fsae_test_number_unit_sample.jsonl`

## Automatic Summary

- Records (extraction/normalization/grounding): 120 / 120 / 120
- Auto quantity coverage (normalized/extracted): 98.17%
- Run duration (s): 7009.5525978000005
- Throughput (records/min): 1.02717

## Structure Metrics (Ground Truth)

- Subject accuracy: 95.26% (201/211)
- Modality accuracy: 97.71% (213/218)
- Condition accuracy: 93.58% (204/218)
- Action accuracy: 92.66% (202/218)
- Object accuracy: 90.23% (194/215)
- Macro structure accuracy: 93.89%

## Quantity & Normalization Metrics (Ground Truth)

- Quantitative constraint precision: 99.38%
- Quantitative constraint recall: 98.17%
- Correct operators / judged: 161/161
- Correct quantities / judged: 160/161
- Correct units / judged: 113/161
- Correct quantity kinds / judged: 122/161
- Correct equivalences / judged: 142/161

## KG Claim Metrics (Ground Truth)

- TP: 60
- FP: 0
- FN: 0
- Precision: 100.00%
- Recall: 100.00%
- F1: 100.00%
- Claim annotation coverage: 1.63%

## Conformance

- Graph source: `prefer_inferred`
- Graphs checked: 119
- Graphs passing all checks: 105 (88.24%)
- Graphs passing error checks: 105 (88.24%)
- Total violations: 16

### Violations by Check

- C01 (error): 0 violations across 0 graphs (0.00%)
- C02 (error): 3 violations across 3 graphs (2.52%)
- C03 (error): 0 violations across 0 graphs (0.00%)
- C04 (error): 0 violations across 0 graphs (0.00%)
- C05 (error): 0 violations across 0 graphs (0.00%)
- C06 (error): 13 violations across 11 graphs (9.24%)

