# Conformance Checks (SPARQL)

This file documents the structural conformance checks implemented in `src/ontology_req_pipeline/evaluation/metrics.py`.

## Objective

Ensure the output graph has the minimum expected structure for:

- requirement backbone,
- quantity/value modeling,
- the core links needed to query and validate final KGs.

## Reporting

For each run, the evaluator reports:

- `% graphs passing all checks`
- `% graphs passing error checks`
- `# total violations`
- per-check `#violations` and `%graphs with violations`

## Prefixes

```sparql
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX iof: <https://spec.industrialontologies.org/ontology/core/Core/>
PREFIX qudt: <http://qudt.org/schema/qudt/>
```

## Checks

### C01 (error): Requirement typed as `iof:RequirementSpecification`

```sparql
SELECT ?req ?spec WHERE {
  ?req iof:requirementSatisfiedBy ?spec .
  FILTER NOT EXISTS { ?req a iof:RequirementSpecification . }
}
```

### C02 (error): Requirement has `iof:requirementSatisfiedBy`

```sparql
SELECT ?req WHERE {
  ?req a iof:RequirementSpecification .
  FILTER NOT EXISTS { ?req iof:requirementSatisfiedBy ?spec . }
}
```

### C03 (error): `requirementSatisfiedBy` target typed as `iof:DesignSpecification` or `iof:PlanSpecification`

```sparql
SELECT ?req ?spec WHERE {
  ?req iof:requirementSatisfiedBy ?spec .
  FILTER NOT EXISTS {
    { ?spec a iof:DesignSpecification . }
    UNION
    { ?spec a iof:PlanSpecification . }
  }
}
```

This check intentionally allows both specification types. In IOF-grounded modeling, requirements about continuants are typically satisfied by `iof:DesignSpecification`, while requirements prescribing occurrents/processes may be satisfied by `iof:PlanSpecification`.

### C04 (error): `qudt:QuantityValue` has a unit

```sparql
SELECT ?value WHERE {
  ?value a qudt:QuantityValue .
  FILTER NOT EXISTS { ?value qudt:unit ?unit . }
}
```

### C05 (error): unit declares `qudt:hasQuantityKind`

```sparql
SELECT ?value ?unit WHERE {
  ?value a qudt:QuantityValue ;
         qudt:unit ?unit .
  FILTER NOT EXISTS { ?unit qudt:hasQuantityKind ?qk . }
}
```

### C06 (error): QuantityValue linked with `isValueExpressionOf*`

```sparql
SELECT ?value WHERE {
  ?value a qudt:QuantityValue .
  FILTER NOT EXISTS {
    { ?value iof:isValueExpressionOfAtSomeTime ?entity . }
    UNION
    { ?value iof:isValueExpressionOfAtAllTimes ?entity . }
  }
}
```

## Rationale Summary

- C01-C03 validate the requirement satisfaction backbone and specification typing.
- C04-C06 validate quantitative-value connectivity (unit, quantity kind, and value-bearing target).
- The reduced set is intentionally minimal: it emphasizes structural correctness of the final KG while avoiding secondary style/traceability checks in the primary conformance score.
