# Evaluation Report

- Generated at (UTC): `2026-04-19T20:42:08.795440+00:00`
- Evaluation directory: `C:\Users\Alessandro Stefanone\repos\req-iof\src\ontology_req_pipeline\evaluation\fsae_test_pipeline_rerun`
- Run ID: `20260413T165250.439956Z`
- Dataset: `fsae_test_number_unit_sample.jsonl`

## Automatic Summary

- Records (extraction/normalization/grounding): 120 / 120 / 120
- Auto quantity coverage (normalized/extracted): 98.78%
- Run duration (s): 6885.419393799999
- Throughput (records/min): 1.045688

## Decomposition Metrics (Ground Truth)

- Decomposition no-error accuracy: 63.74% (160/251)
- Decomposition error rate: 36.25% (91/251)

## Structure Metrics Conditioned on Correct Decomposition

- Scope: 160 rows with `decomposition_error = false`
- Subject accuracy: 98.75% (158/160)
- Modality accuracy: 100.00% (160/160)
- Condition accuracy: 97.50% (156/160)
- Action accuracy: 87.50% (140/160)
- Object accuracy: 88.75% (142/160)
- Conditional macro structure accuracy: 94.50%

## Row-Level Success Patterns

- `decomposition_error = false` and all structure slots correct: 132/160 (82.50%)
- `decomposition_error = false` and all structure slots plus all quantity slots correct: 111/160 (69.38%)
- (`decomposition_error = false` and all structure slots correct) / all requirements: 132/251 (52.59%)
- (`decomposition_error = false` and all structure slots plus all quantity slots correct) / all requirements: 111/251 (44.22%)
- For the quantity-inclusive rows, a row is counted only if all five structure slots are `true`, there are no missing quantitative constraints, and every predicted quantitative constraint is marked `is_true_quantitative_constraint = true` with all quantity sublabels correct. Rows without quantitative constraints count as correct if none are missing.

## Quantity & Normalization Metrics (Ground Truth)

- Quantitative constraint precision: 97.64%
- Quantitative constraint recall: 96.88%
- Correct operators / judged: 120/124
- Correct quantities / judged: 122/124
- Correct units / judged: 114/124
- Correct quantity kinds / judged: 110/124
- Correct equivalences / judged: 111/124

## Conformance

- Graph source: `inferred`
- Post-processing: `RDFLib inverse-object-property materialization from local owl:inverseOf axioms`
- Validation graph set: `final_kgs_inferred_postprocessed_tmp`
- Added inverse object-property assertions: 1023
- Graphs checked: 120
- Graphs passing all checks: 112 (93.33%)
- Graphs passing error checks: 112 (93.33%)
- Total violations: 10

### Violations by Check

- C01 (error): 0 violations across 0 graphs (0.00%)
- C02 (error): 3 violations across 3 graphs (2.50%)
- C03 (error): 7 violations across 5 graphs (4.17%)
- C04 (error): 0 violations across 0 graphs (0.00%)
- C05 (error): 0 violations across 0 graphs (0.00%)
- C06 (error): 0 violations across 0 graphs (0.00%)
