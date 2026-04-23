import json
import textwrap
from typing import Any, Dict, List, Optional, Literal, Tuple
from pathlib import Path
import re
from pydantic import BaseModel, Field
from ollama import Client
from openai import OpenAI
from ontology_req_pipeline.data_models import (
    IndividualRequirement,
    Constraint,
    Record,
    Reference,
    Structure,
    StructureResponse,
    ConstraintsResponse,
    ReferencesResponse,
    IndividualRequirementsResponse,
)

def run_ollama(
    client: Client,
    prompt: str,
    *,
    output_model: Optional[type[BaseModel]] = None,
    model: str = "llama3.2",
    options: Optional[Dict[str, Any]] = None,
) -> Any:
    messages = [
        {"role": "system", "content": "Follow the JSON schema strictly; return JSON only; do not add fields; do not include req_idx inside spans; spans must match original_text substrings."},
        {"role": "user", "content": prompt},
    ]
    last_exc: Optional[Exception] = None
    for _ in range(2):
        try:
            response = client.chat(
                model=model,
                messages=messages,
                format=output_model.model_json_schema() if output_model else "json",
                options=options,
            )
            raw = response.message.content
            parsed = json.loads(raw)
            validated = output_model.model_validate(parsed) if output_model else parsed
            return validated
        except Exception as exc:
            last_exc = exc
            continue
    raise RuntimeError(f"Ollama structured response validation failed for model {model}") from last_exc


def run_openai(
    client: OpenAI,
    prompt: str,
    *,
    output_model: Optional[type[BaseModel]] = None,
    model: str = "gpt-5.1",
    options: Optional[Dict[str, Any]] = None,
) -> Any:
    messages = [
        {"role": "system", "content": "Follow the JSON schema strictly; return JSON only; do not add fields; do not include req_idx inside spans; spans must match original_text substrings."},
        {"role": "user", "content": prompt},
    ]
    last_exc: Optional[Exception] = None
    for _ in range(2):
        try:
            response = client.responses.parse(
                model=model,
                input=messages,
                text_format=output_model,
                **(options or {}),
            )
            parsed = response.output_parsed
            if parsed is None:
                raise ValueError("OpenAI returned no parsed structured payload.")
            if output_model and not isinstance(parsed, output_model):
                parsed = output_model.model_validate(parsed)
            return parsed
        except Exception as exc:
            last_exc = exc
            continue
    raise RuntimeError(f"OpenAI structured response validation failed for model {model}") from last_exc


BASE_GUIDANCE = """
You are a systems engineer expert in requirements elicitation.
Always return pure JSON. Offsets are 0-indexed and end-exclusive. All spans must point into the original_text provided.

"""


def _strip_tagged_examples(prompt: str) -> str:
    stripped = re.sub(r"\n\s*<examples>.*?</examples>\s*", "\n", prompt, flags=re.DOTALL)
    stripped = re.sub(r"\n\s*<few-shot-examples>.*?</few-shot-examples>\s*", "\n", stripped, flags=re.DOTALL)
    return re.sub(r"\n{3,}", "\n\n", stripped).strip() + "\n"

def prompt_record_extraction(original_text: str) -> str:
    return textwrap.dedent(f"""
    <task>
    Decompose the input text into atomic prescriptive requirements.
    An atomic requirement expresses exactly ONE obligation:
    one modality + one predicate applied to one subject,
    possibly constrained by qualifiers (conditions, time, ranges).
    </task>

    <definition>
    A requirement MUST be split if and only if:
    - Two or more distinct prescriptive modalities (e.g., shall, must, should, will, is required to)
      govern different predicates, OR
    - Coordinated predicates express independent obligations
      (e.g., "shall X and shall Y"), OR
    - Bullet or numbered list items each express a standalone obligation.

    A requirement MUST NOT be split when:
    - The text contains tolerances, ranges, or uncertainty modifiers
      (e.g., ±10%, between X and Y, at least, no more than),
    - Conditional or temporal clauses constrain a single predicate
      (e.g., when X, during Y, under condition Z),
    - Parenthetical phrases, units, or references only refine an obligation.
    </definition>

    <instructions>
    - Preserve original wording as much as possible.
    - If a later clause omits the subject or modality, inherit it implicitly
      (e.g., "shall X and Y" → two outputs, second may start with "shall Y").
    - Do NOT paraphrase, normalize, or correct grammar.
    - Output JSON ONLY, with the exact schema below.
    - Do NOT include spans, offsets, or any fields other than those specified.

    Output schema:
    {{
      "individual_requirements": [
        {{
          "req_idx": int,
          "text": str
        }}
      ]
    }}
    </instructions>

    <examples>
    Example 1 — Single obligation:
    original_text: "System shall operate at 5 V."
    output:
    {{
      "individual_requirements": [
        {{ "req_idx": 0, "text": "System shall operate at 5 V." }}
      ]
    }}

    Example 2 — Coordinated obligations:
    original_text: "The vehicle shall start within 2 seconds and shall stop within 5 meters."
    output:
    {{
      "individual_requirements": [
        {{ "req_idx": 0, "text": "The vehicle shall start within 2 seconds" }},
        {{ "req_idx": 1, "text": "shall stop within 5 meters" }}
      ]
    }}

    Example 3 — Single obligation with qualifiers:
    original_text: "The pump shall operate at 10 bar during startup."
    output:
    {{
      "individual_requirements": [
        {{ "req_idx": 0, "text": "The pump shall operate at 10 bar during startup." }}
      ]
    }}

    Example 4 — Bullet list:
    original_text:
    "- Battery shall provide 20 kWh.
     - Charger shall support 400 V."
    output:
    {{
      "individual_requirements": [
        {{ "req_idx": 0, "text": "Battery shall provide 20 kWh." }},
        {{ "req_idx": 1, "text": "Charger shall support 400 V." }}
      ]
    }}
    </examples>

    <input>
    original_text: {original_text}
    </input>

    Return JSON only.
    """)

def prompt_structure(ar: Dict[str, Any], original_text: str) -> str:
    return textwrap.dedent(f"""
    {BASE_GUIDANCE}

    <task>
    Given an individual requirement clause, extract ISO/IEC/IEEE 29148 structural slots:
    subject, modality, condition (with EARS pattern), action, and object.
    </task>

    <slot-definitions>
    - subject (Span):
      The entity that bears the obligation (system/component/actor/artifact).
      Extract the minimal noun phrase naming the obligated entity.

    - modality:
      Prescriptive keyword. Allowed ONLY: ["shall","must","should","may","will","is"].

    - condition (Condition):
      A clause that scopes applicability (WHEN / UNDER WHICH STATE / IN RESPONSE TO WHICH EVENT).
      If absent, condition.present=false and the span fields are empty.

    - action (Span):
      The main verb phrase describing what the subject must do.
      Exclude condition text. Include auxiliaries (e.g., "be able to", "be set to") and adverbials.

    - object (Span):
      The explicit target/complement of the action if present (direct object or PP/NP complement that
      provides the target of the action or the constrained phrase).
      If there is no explicit object/complement, return an empty span.
    </slot-definitions>

    <ears-patterns>
    Set condition.EARS_pattern using ONE label:
    - ubiquitous: no explicit condition text
    - event-driven: triggered by an event (when/if/upon/after/in the event that)
    - state-driven: applies in a state (during/while/as long as/when in state)
    - unwanted_behaviors: fault/abnormal response (upon fault/in case of error/failure)
    - optional_features: optional/configuration dependent (if enabled/configured/optional)

    Rules:
    - If NO condition text exists → condition.present=false and EARS_pattern="ubiquitous".
    - Otherwise condition.present=true and choose the most specific pattern supported by text.
    - Do NOT infer implicit conditions.
    </ears-patterns>

    <span-rules>
    - All spans MUST be exact substrings of original_text.
    - Offsets are character offsets in original_text.
    - Do NOT paraphrase or normalize.
    - Do NOT include req_idx inside spans.
    - Do NOT merge condition text into action.

    IMPORTANT: Span has NO default values.
    Therefore you MUST always output subject, action, and object as Span objects.
    If a slot is not explicitly present, output the EMPTY SPAN sentinel:
      {{ "text": "", "start": 0, "end": 0 }}

    For condition:
    - If condition.present=false, set:
        condition.text="", condition.start=0, condition.end=0, condition.EARS_pattern="ubiquitous"
    </span-rules>

    <output-schema>
    {{
      "req_idx": {ar.get('req_idx', 0)},
      "structure": {{
        "subject": {{ "text": str, "start": int, "end": int }},
        "modality": "shall"|"must"|"should"|"may"|"will"|"is",
        "condition": {{
          "present": bool,
          "EARS_pattern": "ubiquitous"|"event-driven"|"unwanted_behaviors"|"state-driven"|"optional_features",
          "text": str,
          "start": int,
          "end": int
        }},
        "action": {{ "text": str, "start": int, "end": int }},
        "object": {{ "text": str, "start": int, "end": int }}
      }}
    }}
    </output-schema>

    <examples>
    Example 1 — Ubiquitous, explicit object:
    original_text: "The system shall operate at 5 V."
    input requirement: "The system shall operate at 5 V."
    output:
    {{
      "req_idx": 0,
      "structure": {{
        "subject": {{ "text": "The system", "start": 0, "end": 10 }},
        "modality": "shall",
        "condition": {{ "present": false, "EARS_pattern": "ubiquitous", "text": "", "start": 0, "end": 0 }},
        "action": {{ "text": "operate", "start": 17, "end": 24 }},
        "object": {{ "text": "at 5 V", "start": 25, "end": 31 }}
      }}
    }}

    Example 2 — Event-driven:
    original_text: "When powered on, the controller shall log faults."
    input requirement: "When powered on, the controller shall log faults."
    output:
    {{
      "req_idx": 0,
      "structure": {{
        "subject": {{ "text": "the controller", "start": 17, "end": 31 }},
        "modality": "shall",
        "condition": {{ "present": true, "EARS_pattern": "event-driven", "text": "When powered on", "start": 0, "end": 15 }},
        "action": {{ "text": "log", "start": 38, "end": 41 }},
        "object": {{ "text": "faults", "start": 42, "end": 48 }}
      }}
    }}

    Example 3 — No explicit object (EMPTY SPAN sentinel):
    original_text: "The pump shall start."
    input requirement: "The pump shall start."
    output:
    {{
      "req_idx": 0,
      "structure": {{
        "subject": {{ "text": "The pump", "start": 0, "end": 8 }},
        "modality": "shall",
        "condition": {{ "present": false, "EARS_pattern": "ubiquitous", "text": "", "start": 0, "end": 0 }},
        "action": {{ "text": "start", "start": 15, "end": 20 }},
        "object": {{ "text": "", "start": 0, "end": 0 }}
      }}
    }}
    </examples>

    <input>
    original_text: {original_text}
    input requirement: {ar.get('text','')}
    </input>

    Return JSON only.
    """)



def prompt_constraints(ar: Dict[str, Any], original_text: str, structure) -> str:
    return textwrap.dedent(f"""
    {BASE_GUIDANCE}

    <task>
    Extract ALL prescriptive constraints expressed in the given individual requirement clause.
    Each output element MUST be a constraint atom: a minimal, self-contained statement that
    constrains an attribute/relation/event via an explicit operator and typed value, grounded
    by an evidence span in original_text.
    </task>

    <inputs>
    - original_text: full source text (offset reference; all spans must refer to this)
    - atomic_requirement: the clause to analyze
    - structure: extracted spans (subject/modality/condition/action/object) to use as anchors
    </inputs>

    <closed-vocabularies>
    Attribute.kind: "quantity" | "enum" | "boolean" | "event" | "relation"
    Value.kind: "quantity" | "enum" | "boolean" | "entity_ref" | "event_ref" | "none"
    Operator (use ONLY one):
      "eq","neq","lt","le","gt","ge","between",
      "one_of","all_of",
      "has_feature","type_is","uses_method",
      "before","after","during","until"
    Group.relation (use ONLY one): "AND" | "OR"
    Target.kind (use ONLY one): "subject"|"object"|"action"|"condition"|"modality"
    </closed-vocabularies>

    <span-rules>
    - evidence.text MUST be an exact substring of original_text.
    - evidence.start/evidence.end are character offsets in original_text.
    - Do NOT paraphrase evidence.
    - All extracted strings inside value.raw_text and ref.text MUST be substrings of original_text.
    </span-rules>

    <operator-mapping-guidelines>
    Map surface forms to canonical operators:
    - "at least", "no less than", "minimum" -> ge
    - "at most", "no more than", "maximum"  -> le
    - "less than" -> lt
    - "greater than" -> gt
    - "between X and Y", "from X to Y" -> between (v1=X, v2=Y)
    - "±", "+/-", "plus or minus" -> quantity.tol (operator stays eq unless it is a range)
    - temporal cues: "before"->before, "after"->after, "during"->during, "until"->until
    - existence/parts/features:
        "shall have/include/with" -> has_feature (relation constraint)
        "shall be made of/type of" -> one_of/type_is depending on form
    </operator-mapping-guidelines>

    <state-vs-feature-disambiguation>
    The phrase "with ..." can express either:
    (A) a required part/feature (component possession), or
    (B) an operating condition / configuration state.

    Use (A) has_feature + entity_ref/MaterialEntity when "with ..." introduces a noun naming a component/part:
      e.g., "with shock absorbers", "with a filter", "with a protective coating".

    Use (B) during + event_ref/Process when "with ..." introduces a state/configuration of an agent or system:
      e.g., "with a driver seated", "with the cover closed", "with the system powered on",
            "with the valve closed", "with the engine off", "with brakes engaged".
    For (B):
      - target.kind MUST be "condition"
      - attribute.kind MUST be "event"
      - value.kind MUST be "event_ref"
      - ref.expected_type MUST be "Process"
    </state-vs-feature-disambiguation>


    <target-selection-guidelines>
    For each constraint, you MUST output target.kind and may output target.ref.

    1) Determine target.kind by where the constrained phrase semantically attaches:
       - If the constraint constrains the system/component named in structure.subject -> target.kind="subject"
       - If it constrains an entity introduced in structure.object -> target.kind="object"
       - If it constrains the action/process in structure.action (timing, rate of the action, ordering) -> target.kind="action"
       - If it constrains the applicability scope (WHEN/DURING/IN STATE) -> target.kind="condition"
       - If it constrains modality strength explicitly ("must" vs "should" etc.) -> target.kind="modality" (rare)

    2) target.ref (optional):
       Use a stable pointer when possible. Use ONLY one of:
       - "SUBJECT" to refer to structure.subject
       - "OBJECT" to refer to structure.object
       - "ACTION" to refer to structure.action
       - "CONDITION" to refer to structure.condition
       - Or "C{{n}}" to refer to another constraint_idx (meaning: this constraint attaches to the entity introduced by constraint n)
       If unsure, omit ref.
    </target-selection-guidelines>

    <grouping-guidelines>
    group is REQUIRED for every constraint.

    - Default: all constraints in the clause belong to group {{ "group_id": "g0", "relation": "AND" }}.
      Rationale: multiple constraints in one requirement clause are conjunctive unless the text states otherwise.

    - If the clause contains explicit OR / alternatives ("or", "either ... or ...", "A or B"):
        Put the alternative constraints in a shared OR group, e.g. {{ "group_id": "g1", "relation": "OR" }}.

    - If you have both AND and OR in the same clause:
        Use multiple groups:
          * One AND group for the always-required constraints (usually g0)
          * One OR group for alternatives (g1)
        A constraint belongs to exactly ONE group (no nesting).
    </grouping-guidelines>

    <depends_on-guidelines>
    depends_on captures hierarchical attachment among constraints (modifier chains).

    Use depends_on when a constraint refines another constraint's introduced entity, typically in patterns like:
    - "X with Y" where Y is a feature/part/property of X
    - "suspension system with shock absorbers ... with wheel travel of 50 mm"
      => constraints about shock absorbers and wheel travel depend_on the constraint that introduced "suspension system"

    Rules:
    - depends_on is the constraint_idx of the parent constraint.
    - If the constraint is independent (directly about subject/action/condition/object slot), set depends_on=null.
    - Use depends_on especially for repeated "with ..." chains and nested noun-phrase modifiers.
    </depends_on-guidelines>

    <qualifiers-guidelines>
    qualifiers are booleans and MUST be present.

    - qualifiers.negated=true ONLY if explicit negation exists in the clause
      (e.g., "shall not", "must not", "not permitted", "prohibited").
      Do NOT infer negation.

    - qualifiers.preferred=true if:
        (a) modality is "should", OR
        (b) explicit preference markers occur ("preferably", "ideally", "recommended").
      Else false.

    - qualifiers.scope_all=true ONLY if explicit universal quantifiers scope the constraint
      ("all", "each", "every"). Else false.
    </qualifiers-guidelines>

    <value-extraction-guidelines>
    Value must match the operator and attribute kind.

    - Quantity constraints:
        value.kind="quantity"
        value.quantity.v1 is required when a numeric value appears.
        value.quantity.v2 required only for "between" ranges.
        value.quantity.tol used for ± tolerances when present.
        value.quantity.unit_text as written (do NOT normalize units beyond special cases below).
        evidence should include the whole quantitative phrase (e.g., "minimum wheel travel of 50 mm").

    - Enum constraints:
        value.kind="enum"
        enum.members_str: list of strings exactly as in text (trim whitespace).
        Use operator "one_of" for "A or B" alternatives (single atom).
        Use operator "all_of" if the text requires all listed members simultaneously.

    - Boolean constraints:
        value.kind="boolean"
        Only set boolean if explicitly stated true/false in the text.

    - References:
        value.kind in {"entity_ref","event_ref"}
        ref.text is the referenced phrase as written.
        expected_type guess ONLY one of:
          "MaterialEntity"|"Process"|"Quality"|"InformationContentEntity"|"Unknown"

    - If you cannot identify a value, use value.kind="none" and set other value fields to null,
      but ONLY if the constraint is still meaningful (e.g., existence/feature requirement).
    </value-extraction-guidelines>

    <output-schema>
    Return JSON ONLY:

    {{
      "req_idx": {ar.get('req_idx', 0)},
      "constraints": [
        {{
          "constraint_idx": int,
          "group": {{ "group_id": str, "relation": "AND"|"OR" }},
          "target": {{ "kind": "subject"|"object"|"action"|"condition"|"modality", "ref": str|null }},
          "evidence": {{ "text": str, "start": int, "end": int }},
          "attribute": {{ "name": str, "kind": "quantity"|"enum"|"boolean"|"event"|"relation" }},
          "operator": "eq"|"neq"|"lt"|"le"|"gt"|"ge"|"between"|"one_of"|"all_of"|"has_feature"|"type_is"|"uses_method"|"before"|"after"|"during"|"until",
          "value": {{
            "kind": "quantity"|"enum"|"boolean"|"entity_ref"|"event_ref"|"none",
            "raw_text": str,
            "quantity": {{ "v1": float|null, "v2": float|null, "tol": float|null, "unit_text": str|null }} | null,
            "enum": {{ "members_str": [str], "members_qty": [{{"v1":float|null,"v2":float|null,"tol":float|null,"unit_text":str|null}}] }} | null,
            "boolean": bool | null,
            "ref": {{ "text": str, "expected_type": "MaterialEntity"|"Process"|"Quality"|"InformationContentEntity"|"Unknown" }} | null
          }},
          "depends_on": int | null,
          "qualifiers": {{ "negated": bool, "preferred": bool, "scope_all": bool }}
        }}
      ]
    }}
    </output-schema>

    <few-shot-examples>
    Example 1 — Multiple conjunctive constraints (default AND group g0), with hierarchy via depends_on:
    original_text: "The vehicle must have a suspension system with shock absorbers and wheel travel of at least 50 mm."
    atomic_requirement: "The vehicle must have a suspension system with shock absorbers and wheel travel of at least 50 mm."
    structure: subject="The vehicle", action="have", object="a suspension system with shock absorbers and wheel travel of at least 50 mm"
    output:
    {{
      "req_idx": 0,
      "constraints": [
        {{
          "constraint_idx": 0,
          "group": {{ "group_id": "g0", "relation": "AND" }},
          "target": {{ "kind": "object", "ref": "OBJECT" }},
          "evidence": {{ "text": "suspension system", "start": 25, "end": 41 }},
          "attribute": {{ "name": "suspension system", "kind": "relation" }},
          "operator": "has_feature",
          "value": {{
            "kind": "entity_ref",
            "raw_text": "suspension system",
            "quantity": null,
            "enum": null,
            "boolean": null,
            "ref": {{ "text": "suspension system", "expected_type": "MaterialEntity" }}
          }},
          "depends_on": null,
          "qualifiers": {{ "negated": false, "preferred": false, "scope_all": false }}
        }},
        {{
          "constraint_idx": 1,
          "group": {{ "group_id": "g0", "relation": "AND" }},
          "target": {{ "kind": "object", "ref": "C0" }},
          "evidence": {{ "text": "shock absorbers", "start": 47, "end": 61 }},
          "attribute": {{ "name": "shock absorbers", "kind": "relation" }},
          "operator": "has_feature",
          "value": {{
            "kind": "entity_ref",
            "raw_text": "shock absorbers",
            "quantity": null,
            "enum": null,
            "boolean": null,
            "ref": {{ "text": "shock absorbers", "expected_type": "MaterialEntity" }}
          }},
          "depends_on": 0,
          "qualifiers": {{ "negated": false, "preferred": false, "scope_all": false }}
        }},
        {{
          "constraint_idx": 2,
          "group": {{ "group_id": "g0", "relation": "AND" }},
          "target": {{ "kind": "object", "ref": "C0" }},
          "evidence": {{ "text": "wheel travel of at least 50 mm", "start": 66, "end": 96 }},
          "attribute": {{ "name": "wheel travel", "kind": "quantity" }},
          "operator": "ge",
          "value": {{
            "kind": "quantity",
            "raw_text": "50 mm",
            "quantity": {{ "v1": 50.0, "v2": null, "tol": null, "unit_text": "mm" }},
            "enum": null,
            "boolean": null,
            "ref": null
          }},
          "depends_on": 0,
          "qualifiers": {{ "negated": false, "preferred": false, "scope_all": false }}
        }}
      ]
    }}

    Example 2 — Explicit OR alternatives (OR group g1), plus an AND constraint (g0):
    original_text: "The housing shall be made of steel or aluminum and have a protective coating."
    atomic_requirement: "The housing shall be made of steel or aluminum and have a protective coating."
    output:
    {{
      "req_idx": 0,
      "constraints": [
        {{
          "constraint_idx": 0,
          "group": {{ "group_id": "g1", "relation": "OR" }},
          "target": {{ "kind": "object", "ref": "OBJECT" }},
          "evidence": {{ "text": "steel or aluminum", "start": 30, "end": 46 }},
          "attribute": {{ "name": "material", "kind": "enum" }},
          "operator": "one_of",
          "value": {{
            "kind": "enum",
            "raw_text": "steel or aluminum",
            "quantity": null,
            "enum": {{ "members_str": ["steel","aluminum"], "members_qty": [] }},
            "boolean": null,
            "ref": null
          }},
          "depends_on": null,
          "qualifiers": {{ "negated": false, "preferred": false, "scope_all": false }}
        }},
        {{
          "constraint_idx": 1,
          "group": {{ "group_id": "g0", "relation": "AND" }},
          "target": {{ "kind": "object", "ref": "OBJECT" }},
          "evidence": {{ "text": "protective coating", "start": 62, "end": 79 }},
          "attribute": {{ "name": "protective coating", "kind": "relation" }},
          "operator": "has_feature",
          "value": {{
            "kind": "entity_ref",
            "raw_text": "protective coating",
            "quantity": null,
            "enum": null,
            "boolean": null,
            "ref": {{ "text": "protective coating", "expected_type": "MaterialEntity" }}
          }},
          "depends_on": null,
          "qualifiers": {{ "negated": false, "preferred": false, "scope_all": false }}
        }}
      ]
    }}

    Example — Condition-as-state introduced by "with ..." (event_ref):
    original_text: "The device shall maintain accuracy of 1 mm with the cover closed."
    atomic_requirement: "The device shall maintain accuracy of 1 mm with the cover closed."
    output includes:
    - accuracy constraint (quantity)
    - condition constraint:
      attribute.kind="event"
      operator="during"
      value.kind="event_ref"
      ref.expected_type="Process"
      evidence="with the cover closed"
    </few-shot-examples>

    <input>
    original_text: {original_text}
    atomic_requirement: {ar.get('text','')}
    structure: {structure.model_dump_json()}
    </input>

    [SPECIAL CASES: QUANTITY NORMALIZATION]
    1) Angles: unit_text="DEG" for degrees, "rad" for radians.
    2) Percentages: unit_text="PERCENT".
    3) Dimensionless: unit_text="DimensionlessUnit".
    4) Fractional inches (e.g., 5/16"): convert to decimal v1 and unit_text="IN".
    5) Mixed units same system (e.g., 5 ft 3 in): convert to a single unit (prefer inches).
    6) Mixed units different systems (e.g., 8mm (5/16")): choose ONE system (prefer SI if present) and convert.

    Return JSON only.
    """)



def prompt_references(
    ar: Dict[str, Any],
    original_text: str,
    structure: Structure,
    constraints: List[Constraint]
) -> str:

    # Keep constraints compact but readable
    constraints_json = (
        json.dumps([c.model_dump() for c in constraints], ensure_ascii=False)
        if constraints is not None
        else "[]"
    )

    return textwrap.dedent(f"""
    {BASE_GUIDANCE}

    <task>
    Extract external references needed to interpret the atomic requirement.
    A "reference" is any mentioned entity or information artifact that is not defined locally
    in the atomic requirement but is required to understand, validate, or ground it downstream
    (e.g., standards, methods, classifications, named procedures/events, component identifiers,
    anaphoric mentions that point to prior context).

    Do NOT hallucinate. If none exist, return an empty list.
    </task>

    <inputs>
    - original_text: full source text (offset reference)
    - atomic_requirement: the clause to analyze
    - structure: spans for agent/modality/condition/predicate/object
    - constraints: extracted constraint atoms (may include entity_ref/event_ref; use these as signals)
    </inputs>

    <what-counts-as-a-reference>
    Create a Reference when the text includes one of the following:
    1) Standards / regulations / specifications (e.g., "ISO 29148", "ASTM D1234", "MIL-STD-810")
       -> expected_type = "InformationContentEntity"
    2) Methods / procedures / protocols / test names (e.g., "per the XYZ method", "using ABC procedure")
       -> expected_type = "Process" (if it denotes an activity) OR "InformationContentEntity"
          (if it denotes a document/protocol). Choose the best fit from text.
    3) Classifications / named taxonomies / compliance classes (e.g., "Class A", "SIL 2", "IP67")
       -> expected_type = "InformationContentEntity" (default) unless clearly a Quality label.
    4) Named components / part numbers / drawing IDs / model identifiers referenced but not defined
       (e.g., "Valve V-101", "P/N 123-456", "Drawing DWG-001")
       -> expected_type = "MaterialEntity"
    5) Named events or operating modes referenced but not defined locally
       (e.g., "shutdown event", "startup sequence", "emergency stop")
       -> expected_type = "Process" (event/occurrence)
    6) Anaphora / deixis referring to prior context (e.g., "it", "they", "this valve", "the above")
       -> expected_type = inferred best guess (often "MaterialEntity" or "InformationContentEntity"),
          but do NOT resolve it; set resolves_to = null.
    </what-counts-as-a-reference>

    <what-does-not-count>
    Do NOT output references for:
    - Generic nouns fully defined by local context ("the system", "the valve") unless anaphoric
      ("it", "they", "this component") or explicitly pointing to earlier content ("the above").
    - Units, numbers, tolerances, or pure quantitative values (those are constraints, not references).
    - Common verbs or adjectives ("operate", "robust") with no external dependency.
    </what-does-not-count>

    <span-and-output-rules>
    - evidence.text MUST be an exact substring of original_text.
    - evidence.start/end are character offsets in original_text.
    - resolves_to MUST be null (do not attempt entity resolution here).
    - Do NOT include req_idx inside spans.
    - Return JSON only matching ReferencesResponse schema.
    </span-and-output-rules>

    <output-schema>
    {{
      "req_idx": {ar.get('req_idx', 0)},
      "references": [
        {{
          "ref_idx": int,
          "evidence": {{ "text": str, "start": int, "end": int }},
          "expected_type": "MaterialEntity" | "Process" | "Quality" | "InformationContentEntity" | "Unknown",
          "resolves_to": null
        }}
      ]
    }}
    </output-schema>

    <few-shot-examples>
    Example 1 — Standard reference:
    original_text: "The system shall comply with ISO 29148."
    atomic_requirement: "The system shall comply with ISO 29148."
    output:
    {{
      "req_idx": 0,
      "references": [
        {{
          "ref_idx": 0,
          "evidence": {{ "text": "ISO 29148", "start": 28, "end": 36 }},
          "expected_type": "InformationContentEntity",
          "resolves_to": null
        }}
      ]
    }}

    Example 2 — Method / procedure reference:
    original_text: "The coating shall be tested according to ASTM D3359."
    atomic_requirement: "The coating shall be tested according to ASTM D3359."
    output:
    {{
      "req_idx": 0,
      "references": [
        {{
          "ref_idx": 0,
          "evidence": {{ "text": "ASTM D3359", "start": 40, "end": 50 }},
          "expected_type": "InformationContentEntity",
          "resolves_to": null
        }}
      ]
    }}

    Example 3 — Component identifier reference:
    original_text: "Valve V-101 shall remain closed during startup."
    atomic_requirement: "Valve V-101 shall remain closed during startup."
    output:
    {{
      "req_idx": 0,
      "references": [
        {{
          "ref_idx": 0,
          "evidence": {{ "text": "V-101", "start": 5, "end": 10 }},
          "expected_type": "MaterialEntity",
          "resolves_to": null
        }},
        {{
          "ref_idx": 1,
          "evidence": {{ "text": "startup", "start": 36, "end": 43 }},
          "expected_type": "Process",
          "resolves_to": null
        }}
      ]
    }}

    Example 4 — Anaphora:
    original_text: "The valve shall be installed upstream. It shall withstand 2 bar."
    atomic_requirement: "It shall withstand 2 bar."
    output:
    {{
      "req_idx": 0,
      "references": [
        {{
          "ref_idx": 0,
          "evidence": {{ "text": "It", "start": 34, "end": 36 }},
          "expected_type": "MaterialEntity",
          "resolves_to": null
        }}
      ]
    }}
    </few-shot-examples>

    <input>
    original_text: {original_text}
    atomic_requirement: {ar.get('text','')}
    structure: {structure.model_dump_json()}
    constraints: {constraints_json}
    </input>

    Return JSON only.
    """)



def split_individual_requirements(
    client: Client,
    original_text: str,
    local=False,
    model="llama3.2",
    include_examples: bool = True,
) -> List[Dict[str, Any]]:
    prompt = prompt_record_extraction(original_text)
    if not include_examples:
        prompt = _strip_tagged_examples(prompt)
    if not local:
        validated = run_openai(client, prompt, output_model=IndividualRequirementsResponse, model=model)
    else:
        validated = run_ollama(client, prompt, output_model=IndividualRequirementsResponse, model=model)
    return [chunk.model_dump() for chunk in validated.individual_requirements]


def extract_structure(
    client: Client,
    ar: Dict[str, Any],
    original_text: str,
    local=False,
    model="llama3.2",
    include_examples: bool = True,
) -> Structure:
    prompt = prompt_structure(ar, original_text)
    if not include_examples:
        prompt = _strip_tagged_examples(prompt)
    if not local:
        validated = run_openai(client, prompt, output_model=StructureResponse, model=model)
    else:
        validated = run_ollama(client, prompt, output_model=StructureResponse, model=model)
    s = validated.structure
    return Structure(subject=s.subject, modality=s.modality, condition=s.condition, action=s.action, object=s.object)


def extract_constraints(
    client: Client,
    ar: Dict[str, Any],
    original_text: str,
    structure: Structure,
    local=False,
    model="llama3.2",
    include_examples: bool = True,
) -> List[Constraint]:
    prompt = prompt_constraints(ar, original_text, structure)
    if not include_examples:
        prompt = _strip_tagged_examples(prompt)
    if not local:
        validated = run_openai(client, prompt, output_model=ConstraintsResponse, model=model)
    else:
        validated = run_ollama(client, prompt, output_model=ConstraintsResponse, model=model)
    return validated.constraints


def extract_references(
    client: Client,
    ar: Dict[str, Any],
    original_text: str,
    structure: Structure,
    constraints: List[Constraint],
    local=False,
    model="llama3.2",
    include_examples: bool = True,
) -> List[Reference]:
    prompt = prompt_references(ar, original_text, structure, constraints)
    if not include_examples:
        prompt = _strip_tagged_examples(prompt)
    if not local:
        validated = run_openai(client, prompt, output_model=ReferencesResponse, model=model)
    else:
        validated = run_ollama(client, prompt, output_model=ReferencesResponse, model=model)
    return validated.references


def process_text(
    client: Client,
    original_text: str,
    idx: int = 0,
    local=False,
    model="llama3.2",
    prompt_style: str = "few_shot",
) -> Record:
    include_examples = prompt_style != "zero_shot"
    atoms = split_individual_requirements(
        client,
        original_text,
        local,
        model=model,
        include_examples=include_examples,
    )
    requirements: List[IndividualRequirement] = []
    for r_i in atoms:
        req_idx = r_i.get("req_idx", len(requirements))
        r_i["req_idx"] = req_idx
        structure = extract_structure(
            client,
            r_i,
            original_text,
            local=local,
            model=model,
            include_examples=include_examples,
        )
        constraints = extract_constraints(
            client,
            r_i,
            original_text,
            structure,
            local=local,
            model=model,
            include_examples=include_examples,
        )
        references = extract_references(
            client,
            r_i,
            original_text,
            structure,
            constraints,
            local=local,
            model=model,
            include_examples=include_examples,
        )
        requirements.append(
            IndividualRequirement(
                req_idx=req_idx,
                structure=structure,
                constraints=constraints,
                references=references,
                raw_text=r_i.get("text"),
            )
        )
    return Record(idx=int(idx), original_text=original_text, requirements=requirements)
