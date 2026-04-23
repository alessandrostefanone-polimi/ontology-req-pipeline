# Inter-Annotator Agreement Report

- Generated at: `2026-04-19T19:20:44.298728+00:00`
- Annotation parent dir: `src\ontology_req_pipeline\evaluation\fsae_test_pipeline_rerun`
- Annotators included: `ground_truth_extraction_items`, `ground_truth_extraction_items_ann1`, `ground_truth_extraction_items_ann2`, `ground_truth_extraction_items_ann3`, `ground_truth_extraction_items_ann4`

## Requirement-Level Labels Aggregate

| Labels | Total ratings | Total items >=2 raters | Macro pairwise agreement | Macro alpha | Macro weighted mean kappa |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 6 | 1746 | 387 | 80.4% | 0.128264 | 0.18522 |

## Requirement-Level Labels

| Label | Ratings | Items >=2 raters | Pairwise agreement | Alpha | Weighted mean kappa |
| --- | ---: | ---: | ---: | ---: | ---: |
| `decomposition_error` | 371 | 77 | 67.2% | 0.196927 | 0.221695 |
| `subject_correct` | 275 | 62 | 79.4% | 0.105609 | 0.188295 |
| `modality_correct` | 275 | 62 | 97.1% | -0.013072 | 0.0 |
| `condition_correct` | 275 | 62 | 76.5% | -0.025735 | 0.094999 |
| `action_correct` | 275 | 62 | 85.3% | 0.28544 | 0.342977 |
| `object_correct` | 275 | 62 | 77.2% | 0.220414 | 0.263352 |

## Quantity-Constraint Labels Aggregate

| Labels | Total ratings | Total items >=2 raters | Macro pairwise agreement | Macro alpha | Macro weighted mean kappa |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 6 | 1378 | 331 | 94.6% | 0.205702 | 0.239426 |

## Quantity-Constraint Labels

| Label | Ratings | Items >=2 raters | Pairwise agreement | Alpha | Weighted mean kappa |
| --- | ---: | ---: | ---: | ---: | ---: |
| `is_true_quantitative_constraint` | 233 | 56 | 97.2% | -0.006711 | 0.0 |
| `operator_correct` | 229 | 55 | 94.2% | 0.255319 | 0.174006 |
| `quantity_value_correct` | 229 | 55 | 98.6% | 0.0 | 0.0 |
| `unit_correct` | 229 | 55 | 98.6% | 0.588811 | 0.818182 |
| `quantity_kind_correct` | 229 | 55 | 91.4% | 0.059952 | 0.080907 |
| `equivalence_correct` | 229 | 55 | 87.8% | 0.336842 | 0.363459 |

## Requirement-Level Labels Pairwise Details

### `decomposition_error`
| Pair | Overlap | Agreement | Kappa |
| --- | ---: | ---: | ---: |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann1 | 30 | 80.0% | 0.454545 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann2 | 30 | 50.0% | -0.086957 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann3 | 30 | 73.3% | 0.189189 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann4 | 30 | 83.3% | 0.590164 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann2 | 3 | 66.7% | -0.0 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann3 | 10 | 50.0% | 0.0 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann4 | 5 | 60.0% | 0.166667 |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann3 | 11 | 36.4% | 0.0 |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann4 | 5 | 40.0% | 0.0 |
| ground_truth_extraction_items_ann3 vs ground_truth_extraction_items_ann4 | 20 | 75.0% | 0.166667 |

### `subject_correct`
| Pair | Overlap | Agreement | Kappa |
| --- | ---: | ---: | ---: |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann1 | 25 | 96.0% | 0.647887 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann2 | 21 | 71.4% | 0.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann3 | 19 | 100.0% | n/a |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann4 | 22 | 77.3% | 0.0 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann2 | 3 | 66.7% | 0.0 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann3 | 8 | 87.5% | 0.0 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann4 | 5 | 100.0% | 1.0 |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann3 | 10 | 50.0% | 0.0 |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann4 | 5 | 60.0% | 0.166667 |
| ground_truth_extraction_items_ann3 vs ground_truth_extraction_items_ann4 | 18 | 61.1% | 0.0 |

### `modality_correct`
| Pair | Overlap | Agreement | Kappa |
| --- | ---: | ---: | ---: |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann1 | 25 | 100.0% | n/a |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann2 | 21 | 95.2% | 0.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann3 | 19 | 100.0% | n/a |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann4 | 22 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann2 | 3 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann3 | 8 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann4 | 5 | 80.0% | 0.0 |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann3 | 10 | 100.0% | n/a |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann4 | 5 | 100.0% | n/a |
| ground_truth_extraction_items_ann3 vs ground_truth_extraction_items_ann4 | 18 | 88.9% | 0.0 |

### `condition_correct`
| Pair | Overlap | Agreement | Kappa |
| --- | ---: | ---: | ---: |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann1 | 25 | 92.0% | 0.468085 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann2 | 21 | 57.1% | 0.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann3 | 19 | 94.7% | 0.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann4 | 22 | 68.2% | -0.084507 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann2 | 3 | 66.7% | 0.0 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann3 | 8 | 87.5% | 0.0 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann4 | 5 | 80.0% | 0.0 |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann3 | 10 | 50.0% | 0.0 |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann4 | 5 | 80.0% | 0.615385 |
| ground_truth_extraction_items_ann3 vs ground_truth_extraction_items_ann4 | 18 | 77.8% | 0.0 |

### `action_correct`
| Pair | Overlap | Agreement | Kappa |
| --- | ---: | ---: | ---: |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann1 | 25 | 84.0% | 0.519231 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann2 | 21 | 95.2% | 0.644068 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann3 | 19 | 84.2% | 0.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann4 | 22 | 90.9% | 0.62069 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann2 | 3 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann3 | 8 | 75.0% | 0.0 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann4 | 5 | 80.0% | 0.545455 |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann3 | 10 | 90.0% | 0.0 |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann4 | 5 | 80.0% | 0.545455 |
| ground_truth_extraction_items_ann3 vs ground_truth_extraction_items_ann4 | 18 | 72.2% | 0.0 |

### `object_correct`
| Pair | Overlap | Agreement | Kappa |
| --- | ---: | ---: | ---: |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann1 | 25 | 84.0% | 0.519231 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann2 | 21 | 85.7% | 0.350515 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann3 | 19 | 84.2% | 0.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann4 | 22 | 81.8% | 0.505618 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann2 | 3 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann3 | 8 | 62.5% | 0.0 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann4 | 5 | 80.0% | 0.545455 |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann3 | 10 | 60.0% | 0.0 |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann4 | 5 | 60.0% | 0.166667 |
| ground_truth_extraction_items_ann3 vs ground_truth_extraction_items_ann4 | 18 | 61.1% | 0.0 |

## Quantity-Constraint Labels Pairwise Details

### `is_true_quantitative_constraint`
| Pair | Overlap | Agreement | Kappa |
| --- | ---: | ---: | ---: |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann1 | 21 | 95.2% | 0.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann2 | 16 | 100.0% | n/a |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann3 | 27 | 96.3% | 0.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann4 | 22 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann2 | 3 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann3 | 12 | 91.7% | 0.0 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann4 | 5 | 100.0% | n/a |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann3 | 8 | 100.0% | n/a |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann4 | 4 | 100.0% | n/a |
| ground_truth_extraction_items_ann3 vs ground_truth_extraction_items_ann4 | 25 | 96.0% | 0.0 |

### `operator_correct`
| Pair | Overlap | Agreement | Kappa |
| --- | ---: | ---: | ---: |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann1 | 20 | 90.0% | -0.052632 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann2 | 16 | 100.0% | n/a |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann3 | 26 | 92.3% | 0.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann4 | 22 | 95.5% | 0.77551 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann2 | 3 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann3 | 11 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann4 | 5 | 100.0% | n/a |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann3 | 8 | 100.0% | n/a |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann4 | 4 | 100.0% | n/a |
| ground_truth_extraction_items_ann3 vs ground_truth_extraction_items_ann4 | 24 | 87.5% | 0.0 |

### `quantity_value_correct`
| Pair | Overlap | Agreement | Kappa |
| --- | ---: | ---: | ---: |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann1 | 20 | 100.0% | n/a |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann2 | 16 | 100.0% | n/a |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann3 | 26 | 100.0% | n/a |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann4 | 22 | 95.5% | 0.0 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann2 | 3 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann3 | 11 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann4 | 5 | 100.0% | n/a |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann3 | 8 | 100.0% | n/a |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann4 | 4 | 100.0% | n/a |
| ground_truth_extraction_items_ann3 vs ground_truth_extraction_items_ann4 | 24 | 95.8% | 0.0 |

### `unit_correct`
| Pair | Overlap | Agreement | Kappa |
| --- | ---: | ---: | ---: |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann1 | 20 | 100.0% | n/a |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann2 | 16 | 87.5% | 0.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann3 | 26 | 100.0% | 1.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann4 | 22 | 100.0% | 1.0 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann2 | 3 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann3 | 11 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann4 | 5 | 100.0% | n/a |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann3 | 8 | 100.0% | n/a |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann4 | 4 | 100.0% | n/a |
| ground_truth_extraction_items_ann3 vs ground_truth_extraction_items_ann4 | 24 | 100.0% | 1.0 |

### `quantity_kind_correct`
| Pair | Overlap | Agreement | Kappa |
| --- | ---: | ---: | ---: |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann1 | 20 | 90.0% | -0.052632 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann2 | 16 | 81.2% | 0.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann3 | 26 | 96.2% | 0.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann4 | 22 | 90.9% | 0.463415 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann2 | 3 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann3 | 11 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann4 | 5 | 80.0% | 0.0 |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann3 | 8 | 100.0% | n/a |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann4 | 4 | 100.0% | n/a |
| ground_truth_extraction_items_ann3 vs ground_truth_extraction_items_ann4 | 24 | 87.5% | 0.0 |

### `equivalence_correct`
| Pair | Overlap | Agreement | Kappa |
| --- | ---: | ---: | ---: |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann1 | 20 | 95.0% | 0.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann2 | 16 | 87.5% | 0.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann3 | 26 | 100.0% | 1.0 |
| ground_truth_extraction_items vs ground_truth_extraction_items_ann4 | 22 | 77.3% | -0.078431 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann2 | 3 | 100.0% | n/a |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann3 | 11 | 100.0% | 1.0 |
| ground_truth_extraction_items_ann1 vs ground_truth_extraction_items_ann4 | 5 | 80.0% | 0.545455 |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann3 | 8 | 100.0% | 1.0 |
| ground_truth_extraction_items_ann2 vs ground_truth_extraction_items_ann4 | 4 | 50.0% | 0.0 |
| ground_truth_extraction_items_ann3 vs ground_truth_extraction_items_ann4 | 24 | 75.0% | 0.142857 |
