"""Pydantic data models for requirement extraction outputs."""

from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from typing import Any, Dict, List, Optional, Literal, Tuple

class Span(BaseModel):
    text: str
    start: int
    end: int


class Condition(BaseModel):
    present: bool = False
    EARS_pattern: Literal["ubiquitous", "event-driven", "unwanted_behaviors", "state-driven", "optional_features"] = "ubiquitous"
    text: str = ""
    start: int = 0
    end: int = 0


class Attribute(BaseModel):
    name: str
    kind: Literal["quantity", "enum", "boolean", "event", "relation"]


class ValueQuantity(BaseModel):
    v1: Optional[float] = None
    v2: Optional[float] = None
    tol: Optional[float] = None
    unit_text: Optional[str] = None


class ValueEnum(BaseModel):
    members_str: List[str] = Field(default_factory=list)
    members_qty: List[ValueQuantity] = Field(default_factory=list)


class ValueRef(BaseModel):
    text: str
    expected_type: Literal[
        "MaterialEntity",
        "Process",
        "Quality",
        "InformationContentEntity",
        "Unknown",
    ]


class Value(BaseModel):
    kind: Literal["quantity", "enum", "boolean", "entity_ref", "event_ref", "none"]
    raw_text: str = ""
    quantity: Optional[ValueQuantity] = None
    enum: Optional[ValueEnum] = None
    boolean: Optional[bool] = None
    ref: Optional[ValueRef] = None


class Qualifiers(BaseModel):
    negated: bool = False
    preferred: bool = False
    scope_all: bool = False

class Group(BaseModel):
    group_id: str
    relation: Literal["AND", "OR"]

class Target(BaseModel):
    kind: Literal["subject","object","action","condition","modality"]
    ref: Optional[str] = None  # stable ID or pointer

class Constraint(BaseModel):
    constraint_idx: int
    group: Group
    target: Target
    evidence: Span
    attribute: Attribute
    operator: Literal[
        "eq",
        "neq",
        "lt",
        "le",
        "gt",
        "ge",
        "between",
        "one_of",
        "all_of",
        "has_feature",
        "type_is",
        "uses_method",
        "before",
        "after",
        "during",
        "until",
    ]
    value: Value
    depends_on: Optional[int] = None  # constraint_idx of the constraint this one depends on
    qualifiers: Qualifiers


class Structure(BaseModel):
    subject: Span
    modality: Literal["shall", "must", "should", "may", "will", "is"]
    condition: Condition = Field(default_factory=Condition)
    action: Span
    object: Span

class NormalizedQuantity(BaseModel):
    """Internal normalized quantity representation used by the grounder."""
    constraint_idx: int
    quantity_kind_uri: Optional[str] = None
    best_unit_uri: Optional[str] = None
    si_value_primary: Optional[float] = None
    si_unit_primary: Optional[str] = None
    si_value_secondary: Optional[float] = None
    si_unit_secondary: Optional[str] = None
    lower_bound: Optional[float] = None
    upper_bound: Optional[float] = None
    lower_bound_included: Optional[bool] = None
    upper_bound_included: Optional[bool] = None

class Reference(BaseModel):
    ref_idx: int
    evidence: Span
    expected_type: Literal[
        "MaterialEntity",
        "Process",
        "Quality",
        "InformationContentEntity",
        "Unknown",
    ]
    resolves_to: Optional[str] = None


class IndividualRequirement(BaseModel):
    req_idx: int
    structure: Structure
    constraints: List[Constraint] = Field(default_factory=list)
    references: List[Reference] = Field(default_factory=list)
    raw_text: Optional[str] = None

class NormalizedIndividualRequirement(BaseModel):
    req_idx: int
    structure: Structure
    constraints: List[Constraint] = Field(default_factory=list)
    references: List[Reference] = Field(default_factory=list)
    raw_text: Optional[str] = None
    normalized_quantities: List[NormalizedQuantity]


class SourceMeta(BaseModel):
    doc_id: Optional[str] = None
    revision: Optional[str] = None
    section: Optional[str] = None
    sentence_id: Optional[str] = None


class Record(BaseModel):
    idx: int
    source: SourceMeta = Field(default_factory=SourceMeta)
    original_text: str
    requirements: List[IndividualRequirement] = Field(default_factory=list)

class NormalizedRecord(BaseModel):
    idx: int
    source: SourceMeta = Field(default_factory=SourceMeta)
    original_text: str
    requirements: List[NormalizedIndividualRequirement] = Field(default_factory=list)

# Step-level response wrappers
class IndividualRequirementChunk(BaseModel):
    req_idx: int
    text: str

class IndividualRequirementsResponse(BaseModel):
    individual_requirements: List[IndividualRequirementChunk]


class StructureResponse(BaseModel):
    req_idx: int
    structure: Structure


class ConstraintsResponse(BaseModel):
    req_idx: int
    constraints: List[Constraint]


class ReferencesResponse(BaseModel):
    req_idx: int
    references: List[Reference]
