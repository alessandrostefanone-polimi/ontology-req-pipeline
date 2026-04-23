This directory contains the curated public datasets kept in the repository.

Included files:

- `fsae_dev_number_unit_sample.jsonl`: published development split for the
  FSAE number-plus-unit sample.
- `fsae_test_number_unit_sample.jsonl`: published test split used by the main
  evaluation examples.
- `techreq_no_fsae.jsonl`: reconstructed public copy of the 20-row technical
  requirements dataset used by the ablation study in
  `src/ontology_req_pipeline/evaluation/techreq_no_fsae_comparison`.
- `public_demo.jsonl`: tiny smoke-test dataset for quick CLI checks.

Larger source corpora and intermediate extracted datasets are intentionally
excluded from the public repository because redistribution and provenance may
differ by source.

Expected JSONL schema for batch commands:

```json
{"idx": 1, "original_text": "The valve shall withstand a pressure of 10 bar."}
```

The JSONL files in this directory are the intended public inputs for the
repository.
