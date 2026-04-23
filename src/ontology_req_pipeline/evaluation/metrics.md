# Evaluation Metrics (Publication-Oriented)

This document defines the metrics implemented in `src/ontology_req_pipeline/evaluation/metrics.py`.

## Ground Truth Files

The script generates two editable templates:

- `ground_truth_extraction.jsonl`
- `ground_truth_claims.jsonl`

Generation command:

```bash
python -m ontology_req_pipeline.evaluation.metrics \
  --evaluation-dir src/ontology_req_pipeline/evaluation \
  --init-ground-truth
```

The extraction template is requirement-centric and lets you mark whether `subject`, `modality`, `condition`, `action`, and `object` are correct, plus quantitative-constraint correctness.

The claims template is triple-centric and lets you mark each predicted claim as sentence-supported (`true`) or unsupported (`false`), and add missing gold claims.

## Labeling Criteria

Use **semantic equivalence**, not exact string match.

- Mark `true` when predicted content preserves the same meaning/constraint, even if wording differs.
- Mark `false` when meaning, scope, directionality, or constraint logic changes.

### Extraction labels (`ground_truth_extraction.jsonl`)

- `subject_correct`, `action_correct`, `object_correct`, `condition_correct`:
  `true` if semantically equivalent and same scope.
  Minor trailing reference bleed or bullet markers may still be marked `true` when they are semantically inert and do not change the meaning or scope of the requirement.
- `decomposition_error`:
  `true` if the decomposition step produced a wrong atomic requirement split for this row; `false` if the decomposition is acceptable.
  For list-intro rows, mark `false` when the intro is semantically complete or self-explanatory as a requirement on its own.
  Mark `true` when the bullet points or list items are necessary to complete the requirement semantics, so the intro row alone is not a valid atomic requirement.
- `modality_correct`:
  `true` if deontic force is equivalent (`must`/`shall`), `false` if weakened/changed (`must` -> `should`).
- `is_true_quantitative_constraint`:
  `true` if there is a real measurable constraint on the intended target.
- `operator_correct`:
  `true` for equivalent logic (`>=` / `at least` / `no less than`).
- `quantity_value_correct`:
  `true` if numerically equivalent after conversion (e.g., `75%` and `0.75`).
- `unit_correct`:
  `true` if unit is correct/equivalent for the quantity.
- `quantity_kind_correct`:
  `true` if quantity kind is semantically appropriate for the constraint.
- `equivalence_correct`:
  `true` if the conversion from the extracted quantity/unit to the corresponding normalized SI value/unit is correct.
  This field evaluates normalization equivalence only; it does not judge whether the extracted quantity fully preserves the source semantics.

### Claim labels (`ground_truth_claims.jsonl`)

- `supported_by_sentence = true`:
  triple is explicitly stated or clearly entailed by sentence + deterministic pipeline mapping.
- `supported_by_sentence = false`:
  triple is hallucinated, over-specific, directionally wrong, or unsupported by sentence meaning.
- `missing_gold_claims`:
  add supported triples that should exist but are absent from predicted claims.

## 1. Extraction Metrics

### 1.0 Decomposition quality

- `decomposition_error = true` means the raw source sentence was split into atomic requirements incorrectly.
- `decomposition_error = false` means the decomposition for that annotated row is acceptable.
- For list-intro rows, use `decomposition_error = false` if the intro itself is a complete or self-explanatory requirement.
- Use `decomposition_error = true` if the following list items are required to complete the meaning of the requirement.

Metrics:

- `DecompositionErrorRate = DecompositionErrors / JudgedDecompositions`
- `DecompositionAccuracy = CorrectDecompositions / JudgedDecompositions`

### 1.1 Structure-level accuracy

For each slot in `{subject, modality, condition, action, object}`:

- `Accuracy(slot) = Correct(slot) / Judged(slot)`

Macro structure accuracy:

- `MacroAccuracy = mean(Accuracy(subject), ..., Accuracy(object))`

Only judged boolean labels are included.

### 1.2 Quantitative constraint identification

- `ExtractedQty`: number of extracted quantitative constraints (from annotated rows)
- `CorrectExtractedQty`: extracted quantitative constraints marked as true
- `MissingQty`: manually added missing quantitative constraints
- `GoldQty = CorrectExtractedQty + MissingQty`

Metrics:

- `QtyPrecision = CorrectExtractedQty / JudgedExtractedQty`
- `QtyRecall = CorrectExtractedQty / GoldQty`

### 1.3 Constraint-field correctness (on true quantitative constraints)

- `OperatorAccuracy = CorrectOperators / JudgedOperators`
- `QuantityValueAccuracy = CorrectValues / JudgedValues`
- `UnitAccuracy = CorrectUnits / JudgedUnits`

## 2. Normalization Metrics

### 2.1 Automatic pipeline coverage

- `NormalizedOverExtracted = #normalized_quantities / #extracted_quantities`

### 2.2 Ground-truth correctness

On true quantitative constraints:

- `QuantityKindAccuracy = CorrectQuantityKinds / JudgedQuantityKinds`
- `UnitNormalizationAccuracy = CorrectUnits / JudgedUnits`
- `EquivalenceAccuracy = CorrectEquivalences / JudgedEquivalences`

`equivalence_correct` should capture whether numeric/unit conversion semantics are correct.

## 3. End-to-End KG Claim Metrics

Because the true-negative space is effectively unbounded for open-world RDF KGs, we do **not** report TN-based metrics.

Definitions:

- `TP`: predicted claim marked sentence-supported
- `FP`: predicted claim marked not sentence-supported
- `FN`: missing gold claim manually added in `missing_gold_claims`

Metrics:

- `Precision = TP / (TP + FP)`
- `Recall = TP / (TP + FN)`
- `F1 = 2 * Precision * Recall / (Precision + Recall)`

Additional reporting:

- claim annotation coverage = annotated predicted claims / total predicted claims.

## 4. Conformance Metrics

Conformance is computed via SPARQL checks over output graphs:

- `% graphs passing all checks`
- `% graphs passing error checks only`
- `# total violations`
- per-check: `#violations`, `#graphs with violations`, `%graphs with violations`

Conformance logic and queries are documented in `src/ontology_req_pipeline/evaluation/conformance.md`.

## 5. Notes for Reporting

- Report both automatic coverage metrics and ground-truth metrics.
- Always report annotation coverage to make results auditable.
- For small samples, include counts alongside percentages.
