from owlapy.iri import IRI  
from owlapy.owl_ontology import SyncOntology, Ontology
from owlapy.owl_reasoner import SyncReasoner  
from owlapy.owl_axiom import OWLObjectPropertyAssertionAxiom, OWLPropertyAssertionAxiom, OWLClassAssertionAxiom, OWLDataPropertyAssertionAxiom, OWLClass, OWLDeclarationAxiom
from owlapy.owl_property import OWLObjectProperty, OWLDataProperty
from owlapy.owl_individual import OWLNamedIndividual
from owlapy.owl_literal import OWLLiteral
from typing import Tuple, Any, Dict, List, Optional
import json
import re
import rdflib, networkx as nx
from rdflib import URIRef, Literal, Namespace
from rdflib.namespace import RDF, RDFS
from pyvis.network import Network
from pathlib import Path
from tempfile import NamedTemporaryFile

from ontology_req_pipeline.data_models import NormalizedRecord, Record, Span, Condition

COLOR_COMMON = "#1f77b4"   # blue
COLOR_INFERRED = "#d62728" # red
COLOR_BASE_ONLY = "#999999"  # grey

class Req_Template_Instantiation():
    def __init__(self, tbox_path: str, record: Record | NormalizedRecord, reasoner="Pellet") -> None:
        self.tbox_path = tbox_path
        self._tbox_axioms = self._load_tbox_axioms()
        self.reasoner = reasoner
        self.record = record
        self.base_ontology = None
        self.inferred_ontology = None
        ######### NAMESPACES #########

        self.base = SyncOntology(IRI.create(f"http://example.org/req/{self.record.idx}/"), load=False)
        self.base_namespace = f"http://example.org/req/{self.record.idx}/#"
        self.iof_namespace = "https://spec.industrialontologies.org/ontology/core/Core/"
        self.bfo_namespace = "http://purl.obolibrary.org/obo/"
        self.qudt_namespace = "http://qudt.org/schema/qudt/"
        self.qudt_qk_namespace = "http://qudt.org/vocab/quantitykind/"
        self.qudt_unit_namespace = "http://qudt.org/vocab/unit/"
        
        ######### CLASS DECLARATIONS #########
        self.req_class = OWLClass(IRI(self.iof_namespace, "RequirementSpecification"))
        self.designSpec_class = OWLClass(IRI(self.iof_namespace, "DesignSpecification"))
        self.matArtifact_class = OWLClass(IRI(self.iof_namespace, "MaterialArtifact"))
        self.attribute_class = OWLClass(IRI(self.iof_namespace, "BFO_0000019"))  # Quality
        self.valueExpr_class = OWLClass(IRI(self.iof_namespace, "ValueExpression"))
        self.quantityValue_class = OWLClass(IRI(self.qudt_namespace, "QuantityValue"))
        self.unit_class = OWLClass(IRI(self.qudt_namespace, "Unit"))
        self.qk_class = OWLClass(IRI(self.qudt_qk_namespace, "QuantityKind"))
        self.processChar_class = OWLClass(IRI(self.iof_namespace, "ProcessCharacteristic"))

        ########## PROPERTY DECLARATIONS #########
        self.numericValue_op = OWLDataProperty(IRI(self.qudt_namespace, "numericValue"))
        self.lowerBound_op = OWLDataProperty(IRI(self.qudt_namespace, "lowerBound"))
        self.upperBound_op = OWLDataProperty(IRI(self.qudt_namespace, "upperBound"))
        self.max_inclusive_op = OWLDataProperty(IRI(self.qudt_namespace, "maxInclusive"))
        self.min_inclusive_op = OWLDataProperty(IRI(self.qudt_namespace, "minInclusive"))
        self.hasQuantityKind_op = OWLObjectProperty(IRI(self.qudt_namespace, "hasQuantityKind"))
        self.req_satisfiedBy_op = OWLObjectProperty(IRI(self.iof_namespace, "requirementSatisfiedBy"))
        self.prescribes_op = OWLObjectProperty(IRI(self.iof_namespace, "prescribes"))
        self.unit_op = OWLObjectProperty(IRI(self.qudt_namespace, "unit"))
        self.hasQuality_op = OWLObjectProperty(IRI(self.iof_namespace, "hasQuality"))
        self.hasProcessCharacteristic_op = OWLObjectProperty(IRI(self.iof_namespace, "hasProcessCharacteristic"))
        self.isValueExpressionOfAtAllTimes_op = OWLObjectProperty(IRI(self.iof_namespace, "isValueExpressionOfAtAllTimes"))
        self.hasContinuantPartAtAllTimes_op = OWLObjectProperty(IRI(self.iof_namespace, "hasContinuantPartAtAllTimes"))
        self.isValueExpressionOfAtSomeTime_op = OWLObjectProperty(IRI(self.iof_namespace, "isValueExpressionOfAtSomeTime"))

    def _load_tbox_axioms(self):
        tbox_path = Path(self.tbox_path)
        ontology_dir = tbox_path.resolve().parent
        sources = [tbox_path]
        annotation_vocab = ontology_dir / "AnnotationVocabulary.rdf"
        bfo_core = ontology_dir / "bfo-core.owl"
        if annotation_vocab.exists():
            sources.append(annotation_vocab)
        if bfo_core.exists():
            sources.append(bfo_core)

        axioms = []
        for source in sources:
            axioms.extend(self._load_tbox_axioms_from_source(source))
        return axioms

    def _load_tbox_axioms_from_source(self, tbox_path: Path):
        temp_path = None
        load_path = tbox_path
        try:
            if tbox_path.suffix.lower() in {".rdf", ".owl", ".xml"}:
                text = tbox_path.read_text(encoding="utf-8", errors="ignore")
                stripped = re.sub(r"<owl:imports\b[^>]*/>", "", text, flags=re.IGNORECASE)
                stripped = re.sub(
                    r"<owl:imports\b[^>]*>.*?</owl:imports>",
                    "",
                    stripped,
                    flags=re.IGNORECASE | re.DOTALL,
                )
                if stripped != text:
                    with NamedTemporaryFile("w", suffix=tbox_path.suffix, encoding="utf-8", delete=False) as tmp:
                        tmp.write(stripped)
                        temp_path = Path(tmp.name)
                    load_path = temp_path
            return list(SyncOntology(str(load_path)).get_tbox_axioms())
        finally:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except Exception:
                    pass

    def reason(self) -> Tuple[bool, str, Any]:
        # 1. Create an empty SyncOntology  
        combined = SyncOntology(IRI.create("file:/combined.owl"), load=False)  
        # 2. Add TBox axioms from a TBox file  
        for axiom in self._tbox_axioms:  
            combined.add_axiom(axiom)  
            
        # 3. Add ABox axioms from an ABox file  
        # Save your ABox graph to a file first
        # with open(abox_path, "w", encoding="utf-8") as f:
        #     f.write(g.serialize(format="xml"))
        for axiom in self.base_ontology.get_abox_axioms():  
            combined.add_axiom(axiom)  
        
        # 4. (Optional) Save the combined ontology  
        # combined.save("src/ontology_req_pipeline/outputs/combined.owl")  
        # 5. Perform reasoning with a chosen OWLAPI reasoner  
        try:
            reasoner = SyncReasoner(combined, reasoner=self.reasoner)  # or "Pellet", "ELK", etc.  
            if reasoner.has_consistent_ontology():
                print(f"{self.reasoner} reasoner found the ontology to be consistent.")
            else:
                print(f"{self.reasoner} reasoner found the ontology to be inconsistent.")
            # reasoner.generate_and_save_inferred_class_assertion_axioms("inferred_class_assertions.owl")  
            reasoner.infer_axioms_and_save(
                output_path="src/ontology_req_pipeline/outputs/enriched.owl",
                output_format="ttl",
                inference_types=[
                    "InferredClassAssertionAxiomGenerator", 
                    "InferredSubClassAxiomGenerator", 
                    "InferredDisjointClassesAxiomGenerator", 
                    "InferredEquivalentClassAxiomGenerator", 
                    "InferredEquivalentDataPropertiesAxiomGenerator",
                    "InferredEquivalentObjectPropertyAxiomGenerator", 
                    "InferredInverseObjectPropertiesAxiomGenerator",
                    "InferredSubDataPropertyAxiomGenerator", 
                    "InferredSubObjectPropertyAxiomGenerator",
                    "InferredDataPropertyCharacteristicAxiomGenerator", 
                    "InferredObjectPropertyCharacteristicAxiomGenerator"
                    ],
            )

            return True, f"{self.reasoner} reasoning completed.", combined, reasoner
        except Exception as exc:  # noqa: BLE001
            return False, f"{self.reasoner} reasoning failed: {exc}", combined, reasoner
        
    def get_base_ontology(self) -> Ontology:
        """Return the base ontology after applying the template."""
        if self.base_ontology is None:
            self.base_ontology = self._construct_base_ontology()
        return self.base_ontology
    
    def get_inferred_ontology(self) -> Ontology:
        """Return the inferred ontology after reasoning."""
        if self.base_ontology is None:
            self.base_ontology = self._construct_base_ontology()
        if self.inferred_ontology is None:
            success, msg, onto, reasoner = self.reason()
            if success:
                self.inferred_ontology = onto
            else:
                raise RuntimeError(f"Reasoning failed: {msg}")
        return self.inferred_ontology

    def _construct_base_ontology(self) -> Ontology:
        """Construct the base ontology by applying the template to the input record."""

        for i, requirement in enumerate(self.record.requirements):

            req_iri = IRI(self.base_namespace, f"Req_{i}")
            req_ind = OWLNamedIndividual(req_iri)
            req_axiom = OWLClassAssertionAxiom(req_ind, self.req_class)
            self.base.add_axiom(req_axiom)
            
            if not requirement.normalized_quantities:
                continue  # Skip requirements without normalized quantities

            for j, quantity in enumerate(requirement.normalized_quantities):
                if quantity.si_value_primary is None:
                    continue  # Skip quantities without a primary SI value
                designSpec_iri = IRI(self.base_namespace, f"DesignSpec_{j}")
                designSpec_ind = OWLNamedIndividual(designSpec_iri)
                designSpec_axiom = OWLClassAssertionAxiom(designSpec_ind, self.designSpec_class)
                self.base.add_axiom(designSpec_axiom)

                req_satisfiedBy_axiom = OWLObjectPropertyAssertionAxiom(req_ind, self.req_satisfiedBy_op, designSpec_ind)
                self.base.add_axiom(req_satisfiedBy_axiom)

                value_ind = OWLNamedIndividual(IRI(self.base_namespace, f"Value_{j}"))
                value_axiom = OWLClassAssertionAxiom(value_ind, self.valueExpr_class)
                self.base.add_axiom(value_axiom)

                quantityValue_axiom = OWLClassAssertionAxiom(value_ind, self.quantityValue_class)
                self.base.add_axiom(quantityValue_axiom)

                target = requirement.constraints[quantity.constraint_idx].target.kind
                target_obj = getattr(requirement.structure, target)
                # Extract a human‑readable label from the target element (Span, Condition, or str)
                if isinstance(target_obj, Span):
                    target_label = target_obj.text
                elif isinstance(target_obj, Condition):
                    target_label = target_obj.text
                else:
                    target_label = str(target_obj)
                # Fallback to the target kind if text is missing/empty
                target_label = target_label or target

                if quantity.si_unit_primary is not None:
                    unit_ind = OWLNamedIndividual(IRI(self.qudt_unit_namespace, quantity.si_unit_primary.replace(self.qudt_unit_namespace, '')))
                    unit_assertion = OWLObjectPropertyAssertionAxiom(value_ind, self.unit_op, unit_ind)
                    self.base.add_axiom(unit_assertion)
                elif quantity.best_unit_uri is not None:
                    unit_ind = OWLNamedIndividual(IRI(self.qudt_unit_namespace, quantity.best_unit_uri.replace(self.qudt_unit_namespace, '')))
                    unit_assertion = OWLObjectPropertyAssertionAxiom(value_ind, self.unit_op, unit_ind)
                    self.base.add_axiom(unit_assertion)
                if quantity.quantity_kind_uri:
                    qk_ind = OWLNamedIndividual(IRI(self.qudt_qk_namespace, quantity.quantity_kind_uri.replace(self.qudt_qk_namespace, '')))
                    qk_assertion = OWLObjectPropertyAssertionAxiom(value_ind, self.hasQuantityKind_op, qk_ind)
                    self.base.add_axiom(qk_assertion)
                if quantity.lower_bound is not None:
                    if quantity.lower_bound_included is True:
                        lowerBound_axiom = OWLDataPropertyAssertionAxiom(value_ind, self.min_inclusive_op, OWLLiteral(float(quantity.lower_bound)))
                    else:
                        lowerBound_axiom = OWLDataPropertyAssertionAxiom(value_ind, self.lowerBound_op, OWLLiteral(float(quantity.lower_bound)))
                    self.base.add_axiom(lowerBound_axiom)
                if quantity.upper_bound is not None:
                    if quantity.upper_bound_included is True:
                        upperBound_axiom = OWLDataPropertyAssertionAxiom(value_ind, self.max_inclusive_op, OWLLiteral(float(quantity.upper_bound)))
                    else:
                        upperBound_axiom = OWLDataPropertyAssertionAxiom(value_ind, self.upperBound_op, OWLLiteral(float(quantity.upper_bound)))
                    self.base.add_axiom(upperBound_axiom)

                processChar_ind = None
                quality_ind = None

                target_ind = OWLNamedIndividual(IRI(self.base_namespace, requirement.structure.subject.text.replace(' ', '_')))

                ################### PROCESS-SIDE CHARACTERISTICS (e.g., flow rate) --WIP-- ###################

                # if quantity.si_unit_primary is not None:
                #     # Check if it's a process rate quantity (contains "PER-" prefix)
                #     process_rate_vocab = ["PER-SEC", "PER-MIN", "PER-HR", "PER-DAY"]
                #     is_rate = any(unit in quantity.si_unit_primary for unit in process_rate_vocab)
                    
                #     if is_rate:
                #         processChar = requirement.constraints[quantity.constraint_idx].attribute.name
                #         processChar_ind = OWLNamedIndividual(IRI(self.base_namespace, processChar.replace(' ', '_')))
                #         processChar_axiom = OWLClassAssertionAxiom(processChar_ind, self.processChar_class)
                #         self.base.add_axiom(processChar_axiom)
                #         hasProcessCharacteristic_axiom = OWLObjectPropertyAssertionAxiom(target_ind, self.hasProcessCharacteristic_op, processChar_ind)
                #         self.base.add_axiom(hasProcessCharacteristic_axiom)
                #         ds_processChar_axiom = OWLObjectPropertyAssertionAxiom(designSpec_ind, self.prescribes_op, processChar_ind)
                #         self.base.add_axiom(ds_processChar_axiom)
                        
                #     else:
                #         attribute = requirement.constraints[quantity.constraint_idx].attribute.name
                #         quality_ind = OWLNamedIndividual(IRI(self.base_namespace, attribute.replace(' ', '_')))
                #         quality_axiom = OWLClassAssertionAxiom(quality_ind, self.attribute_class)
                #         self.base.add_axiom(quality_axiom)
                #         hasQuality_axiom = OWLObjectPropertyAssertionAxiom(target_ind, self.hasQuality_op, quality_ind)
                #         self.base.add_axiom(hasQuality_axiom)
                #         ds_quality_axiom = OWLObjectPropertyAssertionAxiom(designSpec_ind, self.prescribes_op, quality_ind)
                #         self.base.add_axiom(ds_quality_axiom)

                ###################################################################################################################

                ds_value_expr_axiom = OWLObjectPropertyAssertionAxiom(designSpec_ind, self.hasContinuantPartAtAllTimes_op, value_ind)
                self.base.add_axiom(ds_value_expr_axiom)

                attribute = requirement.constraints[quantity.constraint_idx].attribute.name
                quality_ind = OWLNamedIndividual(IRI(self.base_namespace, attribute.replace(' ', '_')))
                quality_axiom = OWLClassAssertionAxiom(quality_ind, self.attribute_class)
                self.base.add_axiom(quality_axiom)
                hasQuality_axiom = OWLObjectPropertyAssertionAxiom(target_ind, self.hasQuality_op, quality_ind)
                self.base.add_axiom(hasQuality_axiom)
                ds_quality_axiom = OWLObjectPropertyAssertionAxiom(designSpec_ind, self.prescribes_op, quality_ind)
                ds_subject_axiom = OWLObjectPropertyAssertionAxiom(designSpec_ind, self.prescribes_op, target_ind)
                self.base.add_axiom(ds_quality_axiom)
                self.base.add_axiom(ds_subject_axiom)

                value_target_ind = quality_ind
                # Only assert the value expression link if we created a target to point to
                if value_target_ind is not None:
                    if requirement.structure.condition.present:
                        value_attribute_axiom = OWLObjectPropertyAssertionAxiom(value_ind, self.isValueExpressionOfAtSomeTime_op, value_target_ind)
                    else:
                        value_attribute_axiom = OWLObjectPropertyAssertionAxiom(value_ind, self.isValueExpressionOfAtAllTimes_op, value_target_ind)
                    self.base.add_axiom(value_attribute_axiom)
        return self.base

    def save_aboxes(self) -> None:
        """Save the ABox axioms of the base and inferred ontologies to Turtle files."""
        final_kg = SyncOntology(IRI.create("file:/src/ontology_req_pipeline/outputs/final_kg.owl"), load=False)
        for axiom in self.get_base_ontology().get_abox_axioms():
            final_kg.add_axiom(axiom)
        final_kg.save("src/ontology_req_pipeline/outputs/final_kg.owl")

        final_kg_inferred = SyncOntology(IRI.create("file:/src/ontology_req_pipeline/outputs/final_kg_inferred.owl"), load=False)
        for axiom in self.get_inferred_ontology().get_abox_axioms():
            final_kg_inferred.add_axiom(axiom)
        final_kg_inferred.save("src/ontology_req_pipeline/outputs/final_kg_inferred.owl")


def requirement_to_rdf(record: Record | NormalizedRecord) -> rdflib.Graph:
    """Build a lightweight RDF graph from extracted requirements.

    This helper is intentionally simple and serves examples/tests that only need
    a minimal graph projection before full agentic grounding.
    """
    graph = rdflib.Graph()
    iof = Namespace("https://spec.industrialontologies.org/ontology/core/Core/")
    req_ns = Namespace(f"http://example.org/req/{record.idx}#")
    graph.bind("iof", iof)
    graph.bind("rdfs", RDFS)
    graph.bind("", req_ns)

    requirements = getattr(record, "requirements", []) or []
    if not requirements:
        requirements = [None]

    for req_idx, requirement in enumerate(requirements):
        req_iri = req_ns[f"Req_{req_idx}"]
        graph.add((req_iri, RDF.type, iof.RequirementSpecification))
        if requirement is not None:
            raw_text = getattr(requirement, "raw_text", None) or ""
            if raw_text:
                graph.add((req_iri, RDFS.comment, Literal(raw_text)))

    if getattr(record, "original_text", None):
        graph.add((req_ns["Record"], RDFS.comment, Literal(record.original_text)))

    return graph
