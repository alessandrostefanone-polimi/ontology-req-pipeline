import json
import re

from ollama import Client as OllamaClient
from openai import OpenAI
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from ontology_req_pipeline.data_models import NormalizedQuantity
from rdflib import URIRef

load_dotenv()

DEFAULT_OPENAI_MODEL = "gpt-5.1"
DEFAULT_OLLAMA_MODEL = "llama3.2"
QUDT_UNIT_NS = "http://qudt.org/vocab/unit/"


def _normalize_provider(provider: str) -> str:
    normalized = str(provider or "openai").strip().lower()
    if normalized not in {"openai", "ollama"}:
        raise ValueError("provider must be either 'openai' or 'ollama'")
    return normalized


def _resolve_model(provider: str, model: str | None) -> str:
    if model and str(model).strip():
        return str(model).strip()
    return DEFAULT_OPENAI_MODEL if provider == "openai" else DEFAULT_OLLAMA_MODEL


def _strip_code_fence(text: str) -> str:
    cleaned = str(text or "").strip()
    if not cleaned.startswith("```"):
        return cleaned
    parts = cleaned.split("```")
    if len(parts) >= 3:
        cleaned = parts[1].strip()
        # Drop language tag if present.
        if "\n" in cleaned:
            first, rest = cleaned.split("\n", 1)
            if first.strip().lower() in {"json", "turtle", "owl", "rdf"}:
                cleaned = rest.strip()
    return cleaned


def _strip_few_shot_examples(prompt: str) -> str:
    markers = [
        "Now analyze the new sentence.",
        "Now generate the natural-language query for the new unit.",
        "Now solve the following instance.",
    ]
    if "Few-shot examples:" not in prompt:
        return prompt
    start = prompt.find("Few-shot examples:")
    if start == -1:
        return prompt
    end = -1
    for marker in markers:
        marker_idx = prompt.find(marker, start)
        if marker_idx != -1 and (end == -1 or marker_idx < end):
            end = marker_idx
    if end == -1:
        stripped = prompt[:start]
    else:
        stripped = prompt[:start] + prompt[end:]
    return re.sub(r"\n{3,}", "\n\n", stripped).strip() + "\n"


def _as_qudt_unit_uri(unit: str | None) -> URIRef | None:
    raw = str(unit or "").strip()
    if not raw:
        return None
    if raw.startswith("<") and raw.endswith(">"):
        raw = raw[1:-1].strip()
    if raw.startswith(QUDT_UNIT_NS):
        code = raw[len(QUDT_UNIT_NS):]
    else:
        code = raw
    code = _normalize_unit_token(code)
    if not code or code in {"?", "NULL", "NONE", "NAN"}:
        return None
    # Guard against SPARQL/IRI control characters coming from noisy extraction.
    if any(ch in code for ch in ['"', "'", "{", "}", "<", ">", "^", "|", "`"]):
        return None
    return URIRef(f"{QUDT_UNIT_NS}{code}")


def _normalize_unit_token(unit_token: str | None) -> str:
    token = str(unit_token or "").strip()
    compact = token.replace(" ", "").replace("\\", "")
    compact = compact.replace("µ", "u").replace("μ", "u")
    if not compact:
        return ""

    alias_exact = {
        "%": "PERCENT",
        "pct": "PERCENT",
        "percent": "PERCENT",
        "percentage": "PERCENT",
        "degree": "DEG",
        "degrees": "DEG",
        "radian": "RAD",
        "radians": "RAD",
        "hz": "HZ",
        "khz": "KiloHZ",
        "mhz": "MegaHZ",
        "ghz": "GigaHZ",
        "ma": "MilliA",
        "ua": "MicroA",
        "mm": "MilliM",
        "millim": "MilliM",
        "millimeter": "MilliM",
        "millimeters": "MilliM",
        "millimetre": "MilliM",
        "millimetres": "MilliM",
        "cm": "CentiM",
        "centim": "CentiM",
        "centimeter": "CentiM",
        "centimeters": "CentiM",
        "centimetre": "CentiM",
        "centimetres": "CentiM",
        "m": "M",
        "meter": "M",
        "meters": "M",
        "metre": "M",
        "metres": "M",
        "um": "MicroM",
        "micrometer": "MicroM",
        "micrometers": "MicroM",
        "micrometre": "MicroM",
        "micrometres": "MicroM",
        "kilogram": "KiloGM",
        "kilograms": "KiloGM",
        "kilogm": "KiloGM",
        "gram": "GM",
        "grams": "GM",
        "kilopascal": "KiloPA",
        "kilopascals": "KiloPA",
        "kilopa": "KiloPA",
        "pascal": "PA",
        "pascals": "PA",
        "inch": "IN",
        "inches": "IN",
        "second": "SEC",
        "seconds": "SEC",
        "millisecond": "MilliSEC",
        "milliseconds": "MilliSEC",
        "millisec": "MilliSEC",
        "unitless": "UNITLESS",
        "dimensionless": "UNITLESS",
        "num": "NUM",
    }
    lowered = compact.lower()
    if lowered in alias_exact:
        return alias_exact[lowered]

    return compact.replace(" ", "-").strip()


def _normalize_unit_code(unit: str | None) -> str | None:
    unit_uri = _as_qudt_unit_uri(unit)
    if unit_uri is None:
        return None
    return str(unit_uri).replace(QUDT_UNIT_NS, "")


def _dedupe_preserve_order(values):
    seen = set()
    out = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _units_for_quantity_kind(df, quantity_kind: str | None):
    if not quantity_kind:
        return []
    try:
        matches = df.loc[df["quantity_kind"].astype(str) == str(quantity_kind), "unit"]
    except Exception:
        return []
    units = []
    for value in matches:
        unit = str(value).strip()
        if unit and unit.lower() != "nan":
            units.append(unit)
    return _dedupe_preserve_order(units)


def _fallback_from_unit_code(df, unit: str | None, preferred_qks=None):
    unit_code = _normalize_unit_code(unit)
    if not unit_code:
        return None, []
    unit_code = unit_code.upper()
    try:
        subset = df.loc[
            df["unit"].astype(str).str.upper() == unit_code,
            ["quantity_kind", "unit"],
        ]
    except Exception:
        return None, []
    if subset.empty:
        return None, []

    preferred = {str(qk) for qk in (preferred_qks or [])}
    quantity_kind = None
    for qk in subset["quantity_kind"].astype(str):
        if qk in preferred:
            quantity_kind = qk
            break
    if quantity_kind is None:
        quantity_kind = str(subset.iloc[0]["quantity_kind"])

    units = []
    for value in subset["unit"]:
        unit_value = str(value).strip()
        if unit_value and unit_value.lower() != "nan":
            units.append(unit_value)
    return quantity_kind, _dedupe_preserve_order(units)


def _run_structured_response(client, provider: str, model: str, system_prompt: str, user_prompt: str, output_model: type[BaseModel]):
    last_exc: Exception | None = None
    for _ in range(2):
        try:
            if provider == "openai":
                response = client.responses.parse(
                    model=model,
                    input=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    text_format=output_model,
                )
                parsed = response.output_parsed
                if parsed is None:
                    raise ValueError("OpenAI returned no parsed structured payload.")
                if not isinstance(parsed, output_model):
                    parsed = output_model.model_validate(parsed)
                return parsed

            response = client.chat(
                model=model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                format=output_model.model_json_schema(),
                options={"temperature": 0},
            )
            raw = response.message.content
            parsed = json.loads(_strip_code_fence(raw))
            return output_model.model_validate(parsed)
        except Exception as exc:
            last_exc = exc
            continue

    raise RuntimeError(f"Structured response validation failed for provider={provider} model={model}") from last_exc

def query_qk_by_unit(unit, g):
    unit_uri = _as_qudt_unit_uri(unit)
    if unit_uri is None:
        return []
    query = """
PREFIX qudt: <http://qudt.org/schema/qudt/>
SELECT DISTINCT ?qk
WHERE {
       ?qk qudt:applicableUnit ?unit .
    }
    """
    try:
        ans = g.query(query, initBindings={"unit": unit_uri})
    except Exception:
        return []
    qks = [str(row.qk) for row in ans]
    return qks

def extract_si_units(unit_str, g):
    unit_uri = _as_qudt_unit_uri(unit_str)
    if unit_uri is None:
        return []
    query = """
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX sou: <http://qudt.org/vocab/sou/>

SELECT DISTINCT ?unit
WHERE {
    ?unit qudt:applicableSystem sou:SI .
    FILTER(?unit = ?targetUnit)
}
ORDER BY ?unit
"""
    try:
        ans = g.query(query, initBindings={"targetUnit": unit_uri})
    except Exception:
        return []
    units = [str(row.unit) for row in ans]
    return units

class QUDTQueryResponse(BaseModel):
    nl_query: str = Field(..., description="Natural language query to find the quantity kind for the given unit in QUDT ontology through semantic search.")

class UnitContextResponse(BaseModel):
    explanation: str = Field(..., description="Short explanation of what the extracted unit refers to in the original sentence.")

def explain_extracted_unit(
    client,
    unit,
    input_sentence,
    provider: str = "openai",
    model: str | None = None,
    prompt_style: str = "few_shot",
):
    provider = _normalize_provider(provider)
    selected_model = _resolve_model(provider, model)
    prompt = f"""
You are an expert in physical quantities, units of measurement, and engineering requirements.
Given an input sentence and an extracted unit token (which may be informal or partially specified),
write a short explanation of what the unit represents in that sentence, including the likely
physical quantity, whether it is absolute or relative (e.g., gauge vs absolute pressure),
and what object or phenomenon it describes.

Constraints:
- Output 2-3 concise sentences.
- Keep the explanation focused on how the unit is being used in the provided sentence.
- Do not invent details not implied by the sentence.

Few-shot examples:

Input sentence:
"The vessel shall be designed for 600 psig during normal operation."
Unit: "psig"
Explanation:
"The unit psig is pounds per square inch gauge, indicating pressure relative to atmospheric pressure. Here it describes the vessel's design operating pressure."

Input sentence:
"The signal must settle within 20 ms after activation."
Unit: "ms"
Explanation:
"ms is milliseconds, a unit of time duration. It refers to how quickly the signal must stabilize after activation."

Input sentence:
"Flow shall be at least 5 lpm through the cooling loop."
Unit: "lpm"
Explanation:
"lpm stands for liters per minute, a volumetric flow rate. It specifies the required flow through the cooling loop."

Now analyze the new sentence.

Input sentence:
"{input_sentence}"
Unit: "{unit}"
Explanation:
"""
    if prompt_style == "zero_shot":
        prompt = _strip_few_shot_examples(prompt)
    parsed = _run_structured_response(
        client=client,
        provider=provider,
        model=selected_model,
        system_prompt="You are an expert in physical quantities, units of measurements and ontology-grounded information extraction.",
        user_prompt=prompt,
        output_model=UnitContextResponse,
    )
    return parsed.explanation

def generate_nl_query(
    client,
    unit,
    unit_context=None,
    provider: str = "openai",
    model: str | None = None,
    prompt_style: str = "few_shot",
):
    provider = _normalize_provider(provider)
    selected_model = _resolve_model(provider, model)
    prompt = f"""
You are an expert in physical quantities, units of measurement, and the QUDT ontology.
Your task is to formulate a single concise natural-language query that will be used
for semantic search over a ChromaDB collection of QUDT quantity kinds and their units.

Goal:
Given a QUDT unit code, write a natural-language query that:
1. Expresses what physical quantity the unit measures.
2. Includes your best guess of the QUDT quantity kind name in CamelCase
   (for example: Velocity, Mass, Pressure, Torque, VolumetricFlowRate).
3. Mentions the unit code itself.

The semantic index you are querying contains documents with:
- the quantity kind URI,
- a QUDT-style quantity kind name (e.g. "AbsoluteHumidity", "Work", "Torque"),
- the unit code.

Constraints:
- Output exactly ONE short, grammatically correct sentence.
- Do NOT explain your reasoning.
- Do NOT output any quotation marks around the sentence.
- Explicitly include the guessed QUDT quantity kind name in CamelCase in the sentence.
- Use the provided unit code verbatim.
- Tolerance phrases (e.g., "±10") simply describe allowed variation around a value; they do not change the requirement or operator and should not spawn extra requirements.
- If you are unsure of the exact QUDT class name, choose the most reasonable CamelCase name you can
  (e.g., "LinearVelocity", "AngularVelocity", "Volume", "Power", "Energy").

Additional context about how the unit appears in the input sentence (use this to guide the guessed quantity kind):
{unit_context if unit_context is not None else "No additional context provided."}

Few-shot examples:

Unit: "M-PER-SEC"
Natural-language query:
Find the QUDT quantity kind Velocity that is measured in meters per second (M-PER-SEC).

Unit: "KiloGM"
Natural-language query:
Retrieve the QUDT quantity kind Mass whose unit is kilogram (KiloGM).

Unit: "PA"
Natural-language query:
Find the QUDT quantity kind Pressure that is measured in pascals (PA).

Unit: "N-M"
Natural-language query:
Retrieve the QUDT quantity kind Torque whose unit of measure is newton metre (N-M).

Unit: "L-PER-MIN"
Natural-language query:
Find the QUDT quantity kind VolumetricFlowRate that uses litre per minute (L-PER-MIN) as a unit.

Now generate the natural-language query for the new unit.

Unit: {unit}
Natural-language query:
"""
    if prompt_style == "zero_shot":
        prompt = _strip_few_shot_examples(prompt)
    parsed = _run_structured_response(
        client=client,
        provider=provider,
        model=selected_model,
        system_prompt="You are an expert in physical quantities, units of measurements and ontology-grounded information extraction.",
        user_prompt=prompt,
        output_model=QUDTQueryResponse,
    )
    return parsed.nl_query

class BestUnitResponse(BaseModel):
    best_unit: str = Field(..., description="The best unit chosen from the list of units depending on the context of the input sentence.")

def choose_best_unit(
    client,
    input_sentence,
    extracted_units,
    quantity_kind,
    provider: str = "openai",
    model: str | None = None,
    prompt_style: str = "few_shot",
):
    provider = _normalize_provider(provider)
    selected_model = _resolve_model(provider, model)
    units_str = ", ".join(extracted_units)
    prompt = f"""
You are an expert in physical quantities, engineering requirements, and the QUDT ontology.
Your task is to select the most appropriate SI unit from a given list of QUDT unit URIs,
based on the context provided by an input sentence and the associated quantity kind.

Inputs:
1. An input sentence describing a requirement, constraint, or parameter.
2. A list of candidate SI unit URIs from QUDT. Each URI has the form:
   http://qudt.org/vocab/unit/<UNIT_CODE>
3. The URI of the quantity kind in QUDT.

Your job:
- Choose exactly ONE unit URI from the given list that best fits the meaning and scale
  implied by the input sentence for the given quantity kind.
- Always choose a unit that appears in the provided list; never invent a new URI.
- If multiple units are plausible, choose the one that fits best with respect to the input sentence provided.
- Tolerances (e.g., "±10" or "plus or minus") simply describe allowed variation around the stated value and do not change the underlying requirement or operator.
- Return ONLY the chosen unit URI, with no explanation or extra text.

Few-shot examples:

Example 1
---------
Input sentence:
"The beam length shall be specified with a tolerance of ±0.5."

Quantity kind:
http://qudt.org/vocab/quantitykind/Length

Candidate units:
[
  "http://qudt.org/vocab/unit/M",
  "http://qudt.org/vocab/unit/CentiM",
  "http://qudt.org/vocab/unit/MilliM"
]

Best unit URI:
"http://qudt.org/vocab/unit/MilliM"

(Reasoning, not to be output: Length tolerances in mechanical design are often expressed in millimetres.)

Example 2
---------
Input sentence:
"The design pressure of the vessel shall not exceed 10^6 Pa."

Quantity kind:
http://qudt.org/vocab/quantitykind/Pressure

Candidate units:
[
  "http://qudt.org/vocab/unit/PA",
  "http://qudt.org/vocab/unit/KiloPA",
  "http://qudt.org/vocab/unit/BAR"
]

Best unit URI:
"http://qudt.org/vocab/unit/PA"

(Reasoning, not to be output: PA is the unit code in QUDT for pascals (Pa, which is cited in the input sentence).

Example 3
---------
Input sentence:
"The motor shall provide a torque of at least 50 Nm at the shaft."

Quantity kind:
http://qudt.org/vocab/quantitykind/Torque

Candidate units:
[
  "http://qudt.org/vocab/unit/N-M",
  "http://qudt.org/vocab/unit/N",
  "http://qudt.org/vocab/unit/J"
]

Best unit URI:
"http://qudt.org/vocab/unit/N-M"

(Reasoning, not to be output: Torque in the input sentence is measured in newton metres, not in newtons or joules.)

Now solve the following instance.

Input sentence:
"{input_sentence}"

Quantity kind:
{quantity_kind}

Candidate units:
[{units_str}]

Remember:
- Select exactly ONE URI from the list.
- Answer with the URI only, no explanation and no additional text.
"""
    if prompt_style == "zero_shot":
        prompt = _strip_few_shot_examples(prompt)
    parsed = _run_structured_response(
        client=client,
        provider=provider,
        model=selected_model,
        system_prompt="You are an expert in physical quantities, units of measurements and ontology-grounded information extraction.",
        user_prompt=prompt,
        output_model=BestUnitResponse,
    )
    return parsed.best_unit

def retrieve_qk_properties(qk_uri, g):
    qk = URIRef(str(qk_uri))
    query = """
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?label ?description ?symbol ?dimVec
WHERE {
    OPTIONAL { ?qk rdfs:label ?label . FILTER(langMatches(lang(?label), "en")) }
    OPTIONAL { ?qk qudt:plainTextDescription ?description . FILTER(langMatches(lang(?description), "en")) }
    OPTIONAL { ?qk qudt:symbol ?symbol . }
    OPTIONAL { ?qk qudt:hasDimensionVector ?dimVec . }
}
"""
    try:
        ans = g.query(query, initBindings={"qk": qk})
    except Exception:
        return None
    for row in ans:
        return {
            "label": row.label,
            "description": row.description,
            "symbol": row.symbol,
            "dimVec": row.dimVec,
        }
    return None

def retrieve_unit_properties(unit_uri, g):
    canonical_unit = _as_qudt_unit_uri(str(unit_uri) if unit_uri is not None else None)
    if canonical_unit is None:
        raw = str(unit_uri or "").strip()
        if not raw:
            return None
        unit = URIRef(raw)
    else:
        unit = canonical_unit
    query = """
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?label ?symbol ?abbreviation ?ucumCode ?conversionMultiplier ?conversionOffset
WHERE {
    OPTIONAL { ?unit rdfs:label ?label . FILTER(langMatches(lang(?label), "en")) }
    OPTIONAL { ?unit qudt:symbol ?symbol . }
    OPTIONAL { ?unit qudt:abbreviation ?abbreviation . }
    OPTIONAL { ?unit qudt:ucumCode ?ucumCode . }
    OPTIONAL { ?unit qudt:conversionMultiplier ?conversionMultiplier . }
    OPTIONAL { ?unit qudt:conversionOffset ?conversionOffset . }
}
"""
    try:
        ans = g.query(query, initBindings={"unit": unit})
    except Exception:
        return None
    for row in ans:
        return {
            "label": row.label,
            "symbol": row.symbol,
            "abbreviation": row.abbreviation,
            "ucumCode": row.ucumCode,
            "conversionMultiplier": row.conversionMultiplier,
            "conversionOffset": row.conversionOffset,
        }
    return None

def find_si_unit(unit, g):
    unit_uri = _as_qudt_unit_uri(unit)
    if unit_uri is None:
        return None

    # First, try the explicit scaling link (covers prefixed SI units like kPa).
    scaling_query = """
PREFIX qudt: <http://qudt.org/schema/qudt/>
SELECT DISTINCT ?siUnit
WHERE { ?unit qudt:scalingOf ?siUnit }
"""
    try:
        result = list(g.query(scaling_query, initBindings={"unit": unit_uri}))
    except Exception:
        return None
    if result:
        return result[0].siUnit

    # Otherwise, pick an SI-coherent unit that shares the quantity kind and is part of the SI system.
    qk_query = """
PREFIX qudt: <http://qudt.org/schema/qudt/>
PREFIX sou: <http://qudt.org/vocab/sou/>
SELECT DISTINCT ?siUnit ?mult
WHERE {
    ?unit qudt:hasQuantityKind ?qk .
    ?siUnit qudt:hasQuantityKind ?qk .
    {
        ?siUnit qudt:definedUnitOfSystem sou:SI .
    } UNION {
        ?siUnit qudt:derivedCoherentUnitOfSystem sou:SI .
    } UNION {
        ?siUnit qudt:applicableSystem sou:SI .
    } UNION {
        ?siUnit qudt:siExactMatch ?_exact .
    }
    OPTIONAL { ?siUnit qudt:conversionMultiplier ?mult . }
}
"""

    candidates = []
    try:
        rows = g.query(qk_query, initBindings={"unit": unit_uri})
    except Exception:
        return None
    for row in rows:
        mult = None
        if getattr(row, "mult", None) is not None:
            try:
                mult = float(row.mult)
            except Exception:
                mult = None
        candidates.append((str(row.siUnit), mult))

    if not candidates:
        return None

    # Prefer a unit whose multiplier is 1 (i.e., coherent SI); otherwise pick the closest.
    candidates.sort(key=lambda item: abs(item[1] - 1) if item[1] is not None else float("inf"))
    return candidates[0][0]

def convert_to_SI(value, unit, g):
    if value is None:
        return None, unit

    value = float(value)
    si_unit_uri = find_si_unit(unit, g)
    if si_unit_uri is None:
        return value, unit

    unit_properties = retrieve_unit_properties(unit, g)
    si_unit_properties = retrieve_unit_properties(si_unit_uri, g)

    if unit_properties is None or si_unit_properties is None:
        return value, unit

    if unit_properties["conversionMultiplier"] is not None and unit_properties["conversionOffset"] is not None:
        value_SI = value * float(unit_properties["conversionMultiplier"]) + float(unit_properties["conversionOffset"])
    elif unit_properties["conversionMultiplier"] is not None:
        value_SI = value * float(unit_properties["conversionMultiplier"])
    else:
        value_SI = value

    return value_SI, si_unit_uri

class BestQKResponse(BaseModel):
    best_qk: str = Field(..., description="The best QUDT quantity kind URI chosen from the list of candidates depending on the context of the input sentence.")

def decide_best_qk(
    client,
    input_sentence,
    quantity_kind_candidates,
    provider: str = "openai",
    model: str | None = None,
    prompt_style: str = "few_shot",
):
    provider = _normalize_provider(provider)
    selected_model = _resolve_model(provider, model)
    prompt = f"""
You are an expert in physical quantities, units of measurement, and the QUDT ontology.

Your task is to select the most appropriate QUDT quantity kind from a given list of QUDT quantity kind URIs, based on the context provided by an input sentence.

Inputs:
1. An input sentence describing a requirement, constraint, or parameter.
2. A list of candidate QUDT quantity kind URIs.
Your job:
- Choose exactly ONE quantity kind URI from the given list that best fits the meaning implied by the input sentence.
- Always choose a quantity kind that appears in the provided list; never invent a new URI.
- If multiple quantity kinds are plausible, choose the one that fits best with respect to the input sentence provided.
- Return ONLY the chosen quantity kind URI, with no explanation or extra text.
Few-shot examples:
Example 1
---------
Input sentence:
"The beam length shall be specified with a tolerance of ±0.5."
Quantity kind candidates:
[
  "http://qudt.org/vocab/quantitykind/Length",
  "http://qudt.org/vocab/quantitykind/Mass",
  "http://qudt.org/vocab/quantitykind/Time"
]
Best quantity kind URI:
"http://qudt.org/vocab/quantitykind/Length"

Example 2
---------
Input sentence:
"The design pressure of the vessel shall not exceed 10^6 Pa."
Quantity kind candidates:
[
  "http://qudt.org/vocab/quantitykind/Pressure",
  "http://qudt.org/vocab/quantitykind/Temperature",
  "http://qudt.org/vocab/quantitykind/Volume"
]
Best quantity kind URI:
"http://qudt.org/vocab/quantitykind/Pressure"
Example 3
---------
Input sentence:
"The motor shall provide a torque of at least 50 Nm at the shaft."
Quantity kind candidates:
[
  "http://qudt.org/vocab/quantitykind/Torque",
  "http://qudt.org/vocab/quantitykind/Energy",
  "http://qudt.org/vocab/quantitykind/Force"
]
Best quantity kind URI:
"http://qudt.org/vocab/quantitykind/Torque"
Now solve the following instance.
Input sentence:
"{input_sentence}"
Quantity kind candidates:
[{quantity_kind_candidates}]
Best quantity kind URI:
"""
    if prompt_style == "zero_shot":
        prompt = _strip_few_shot_examples(prompt)
    parsed = _run_structured_response(
        client=client,
        provider=provider,
        model=selected_model,
        system_prompt="You are an expert in physical quantities, units of measurements and ontology-grounded information extraction.",
        user_prompt=prompt,
        output_model=BestQKResponse,
    )
    return parsed.best_qk

def semantic_search_qk(
    client,
    input_text,
    nl_query,
    df,
    collection,
    provider: str = "openai",
    model: str | None = None,
    prompt_style: str = "few_shot",
):
    if collection is None:
        return None, []

    res = collection.query(
        query_texts=[nl_query],
        n_results=10,
    )

    extracted_units = []
    quantities = []
    
    for row in res['metadatas'][0]:
        quantity = row['quantity_kind_uri']
        quantities.append(quantity)

    best_quantity = decide_best_qk(
        client,
        input_text,
        quantities,
        provider=provider,
        model=model,
        prompt_style=prompt_style,
    )

    for index, row in df.iterrows():
        if str(row['quantity_kind']) == best_quantity:
            extracted_units.append(row.unit)

    return best_quantity, extracted_units

def qudt_extraction_wf(
    idx,
    input_text,
    primary_value,
    secondary_value,
    tolerance,
    operator,
    unit,
    df,
    g,
    collection,
    provider: str = "openai",
    model: str | None = None,
    prompt_style: str = "few_shot",
):
    provider = _normalize_provider(provider)
    selected_model = _resolve_model(provider, model)
    client = OpenAI() if provider == "openai" else OllamaClient()
    si_value_secondary = None
    si_unit_secondary = None

    qk_candidates = query_qk_by_unit(unit, g)
    try:
        qk_values_in_df = set(df["quantity_kind"].astype(str))
    except Exception:
        qk_values_in_df = set()
    qk_candidates_in_df = [qk for qk in qk_candidates if str(qk) in qk_values_in_df]

    if not qk_candidates:
        quantity_kind, extracted_units = _fallback_from_unit_code(df, unit)
        if quantity_kind is None or not extracted_units:
            if collection is None:
                return None
            unit_context = explain_extracted_unit(
                client,
                unit,
                input_text,
                provider=provider,
                model=selected_model,
                prompt_style=prompt_style,
            )
            nl_query = generate_nl_query(
                client,
                unit,
                unit_context,
                provider=provider,
                model=selected_model,
                prompt_style=prompt_style,
            )
            quantity_kind, extracted_units = semantic_search_qk(
                client,
                input_text,
                nl_query,
                df,
                collection,
                provider=provider,
                model=selected_model,
                prompt_style=prompt_style,
            )
            if quantity_kind is None or not extracted_units:
                quantity_kind, extracted_units = _fallback_from_unit_code(df, unit)
            if quantity_kind is None or not extracted_units:
                return None
        best_unit = choose_best_unit(
            client,
            input_text,
            extracted_units,
            quantity_kind,
            provider=provider,
            model=selected_model,
            prompt_style=prompt_style,
        )
        qk_prop = retrieve_qk_properties(quantity_kind, g)
        unit_prop = retrieve_unit_properties(best_unit, g)
        si_value_primary, si_unit_primary = convert_to_SI(primary_value, best_unit, g)
        if secondary_value != None and secondary_value != "null":
            si_value_secondary, si_unit_secondary = convert_to_SI(secondary_value, best_unit, g)
    else:
        candidate_pool = qk_candidates_in_df if qk_candidates_in_df else qk_candidates
        quantity_kind = decide_best_qk(
            client,
            input_text,
            candidate_pool,
            provider=provider,
            model=selected_model,
            prompt_style=prompt_style,
        )
        extracted_units = _units_for_quantity_kind(df, quantity_kind)
        if not extracted_units and qk_candidates_in_df:
            for fallback_qk in qk_candidates_in_df:
                fallback_units = _units_for_quantity_kind(df, fallback_qk)
                if fallback_units:
                    quantity_kind = fallback_qk
                    extracted_units = fallback_units
                    break
        if not extracted_units:
            fallback_qk, fallback_units = _fallback_from_unit_code(df, unit, preferred_qks=qk_candidates)
            if fallback_qk is not None and fallback_units:
                quantity_kind = fallback_qk
                extracted_units = fallback_units
        if not extracted_units:
            return None
        best_unit = choose_best_unit(
            client,
            input_text,
            extracted_units,
            quantity_kind,
            provider=provider,
            model=selected_model,
            prompt_style=prompt_style,
        )
        qk_prop = retrieve_qk_properties(quantity_kind, g)
        unit_prop = retrieve_unit_properties(best_unit, g)
        si_value_primary, si_unit_primary = convert_to_SI(primary_value, best_unit, g)
        if secondary_value != None and secondary_value != "null":
            si_value_secondary, si_unit_secondary = convert_to_SI(secondary_value, best_unit, g)

    # Convert tolerance using the same scaling (ignore offsets for tolerances).
    tolerance_si = None
    if tolerance is not None:
        try:
            multiplier = float(unit_prop.get("conversionMultiplier")) if unit_prop and unit_prop.get("conversionMultiplier") is not None else 1.0
        except Exception:
            multiplier = 1.0
        tolerance_si = float(tolerance) * multiplier

    tol = tolerance_si if tolerance_si is not None else 0

    if operator == "gt":
        lower_bound = si_value_primary - tol
        upper_bound = None
        upper_bound_included = False
        lower_bound_included = False
    elif operator == "ge":
        lower_bound = si_value_primary - tol
        upper_bound = None
        upper_bound_included = False
        lower_bound_included = True
    elif operator == "lt":
        lower_bound = None
        upper_bound = si_value_primary + tol
        upper_bound_included = False
        lower_bound_included = False
    elif operator == "le":
        lower_bound = None
        upper_bound = si_value_primary + tol
        upper_bound_included = True
        lower_bound_included = False
    elif operator == "between" and secondary_value != None:
        lower_bound = min(si_value_primary, si_value_secondary)
        upper_bound = max(si_value_primary, si_value_secondary)
        upper_bound_included = True
        lower_bound_included = True
    else:
        lower_bound = si_value_primary - tol
        upper_bound = si_value_primary + tol
        upper_bound_included = True
        lower_bound_included = True
    best_unit_uri = _as_qudt_unit_uri(best_unit)
    si_unit_primary_uri = _as_qudt_unit_uri(str(si_unit_primary) if si_unit_primary is not None else None)
    si_unit_secondary_uri = _as_qudt_unit_uri(str(si_unit_secondary) if si_unit_secondary is not None else None)
    return NormalizedQuantity(
        constraint_idx=idx,
        quantity_kind_uri=quantity_kind,
        quantity_kind_properties=qk_prop,
        best_unit_uri=str(best_unit_uri) if best_unit_uri is not None else best_unit,
        best_unit_properties=unit_prop,
        si_value_primary=si_value_primary,
        si_unit_primary=str(si_unit_primary_uri) if si_unit_primary_uri is not None else si_unit_primary,
        si_value_secondary=si_value_secondary if secondary_value != None else None,
        si_unit_secondary=(
            str(si_unit_secondary_uri) if si_unit_secondary_uri is not None else si_unit_secondary
        ) if secondary_value != None else None,
        lower_bound=lower_bound,
        upper_bound=upper_bound,
        lower_bound_included=lower_bound_included,
        upper_bound_included=upper_bound_included,
    )
