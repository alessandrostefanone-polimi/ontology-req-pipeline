# ontology_req_pipeline

Pipeline for turning natural-language requirements into ontology-grounded RDF/OWL artifacts using:
- LLM-based extraction
- QUDT-oriented quantity normalization
- Agentic IOF/QUDT grounding
- Reasoning and QA reporting over batch runs

## What This Repository Contains

- `src/ontology_req_pipeline/extraction`: requirement decomposition and structured extraction.
- `src/ontology_req_pipeline/normalization`: quantity/unit normalization and QUDT alignment.
- `src/ontology_req_pipeline/ontology`: ontology grounding and reasoning (`AgenticKGBuilder`).
- `src/ontology_req_pipeline/cli.py`: main command-line entry point.
- `ontologies/`: local ontology resources used by grounding (for example `Core.rdf`, `QUDT-all-in-one-OWL.ttl`).
- `datasets/`: input datasets and derived evaluation artifacts.

## Requirements

- Python 3.10+
- For OpenAI provider: `OPENAI_API_KEY`
- For Ollama provider: running Ollama server (default URL `http://localhost:11434/v1`)
- Pellet reasoning backend is used by default via Owlapy (`--reasoner Pellet`)

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -e .
```

If you prefer a requirements-based install:

```powershell
pip install -r requirements.txt
pip install -e .
```

Install development dependencies (including `pytest`):

```powershell
pip install -e ".[dev]"
```

Create a local environment file:

```powershell
Copy-Item .env.example .env
```

## Environment Variables

Common:
- `OPENAI_API_KEY`: required for OpenAI-backed stages.

Ollama:
- `OLLAMA_BASE_URL` (default `http://localhost:11434/v1`)
- `OLLAMA_API_KEY` (default `ollama`)

Optional Chroma Cloud fallback for normalization:
- `CHROMA_API_KEY`
- `CHROMA_TENANT`
- `CHROMA_DATABASE`
- `CHROMA_COLLECTION` (default `qudt_quantity_kinds_with_descriptions`)

If Chroma vars are not set, normalization still runs, but semantic fallback lookup is disabled.

## CLI Usage

Main entry point:

```powershell
python -m ontology_req_pipeline.cli --help
```

### 1) Run a Single Demo Pipeline

```powershell
python -m ontology_req_pipeline.cli run-pipeline
```

`run-pipeline` is a fixed smoke-test flow with an internal example sentence.

### 2) Generate a Labeled Extraction Dataset

```powershell
python -m ontology_req_pipeline.cli generate-labeled-dataset `
  --input-path datasets/extracted_reqs/fsae_test_number_unit_sample.jsonl `
  --output-path src/ontology_req_pipeline/evaluation/labeled_dataset.jsonl `
  --limit 10 `
  --provider openai `
  --model gpt-5.1
```

### 3) Run Full Batch Evaluation Pipeline

```powershell
python -m ontology_req_pipeline.cli run-evaluation-pipeline `
  --input-path datasets/extracted_reqs/fsae_test_number_unit_sample.jsonl `
  --output-dir src/ontology_req_pipeline/evaluation `
  --provider openai `
  --model gpt-5.1 `
  --normalization-provider openai `
  --grounding-provider openai `
  --reasoner Pellet
```

You can switch any stage to local Ollama:

```powershell
python -m ontology_req_pipeline.cli run-evaluation-pipeline `
  --provider ollama `
  --model llama3.2 `
  --normalization-provider ollama `
  --grounding-provider ollama
```

### 4) Recompute QA Report on Existing Outputs

```powershell
python -m ontology_req_pipeline.cli qa-evaluation-report `
  --output-dir src/ontology_req_pipeline/evaluation
```

## Input Data Format

For CLI batch commands, each JSONL row should contain:
- `original_text` (required)
- `idx` (optional; fallback index is used if missing)

Minimal example:

```json
{"idx": 1, "original_text": "The valve shall withstand a pressure of 10 bar."}
```

## Output Artifacts

`run-evaluation-pipeline` writes, at minimum:
- `extraction.jsonl`
- `normalization.jsonl`
- `grounding.jsonl`
- `run_metadata.json`
- `qa_report.json`
- `qa_report.md`
- per requirement KG files like `final_kg_<idx>.ttl` and `final_kg_inferred_<idx>.ttl`

## Python API (Programmatic Use)

```python
from ontology_req_pipeline.extraction.llm_extractor import get_default_extractor
from ontology_req_pipeline.normalization.qudt_normalization import normalize_qudt
from ontology_req_pipeline.ontology.agentic_kg_builder import AgenticKGBuilder

extractor = get_default_extractor()
record = extractor.extract(
    "The valve shall have a flow rate of 100 +- 2 liters per minute.",
    local=False,
    idx=0,
    model="gpt-5.1",
)

normalized = normalize_qudt(
    idx=record.idx,
    input_text=record.original_text,
    requirements=record.requirements,
    provider="openai",
    model="gpt-5.1",
)

builder = AgenticKGBuilder(
    tbox_path="ontologies/Core.rdf",
    record=normalized,
    reasoner="Pellet",
    llm_provider="openai",
    llm_model="gpt-5.1",
)
result = builder.two_stage_workflow()
print(result["output_paths"])
```

## Notes

- This repository contains research and experiment artifacts (datasets, evaluations, outputs, notebooks). Not all folders are part of the production CLI path.
- Grounding/reasoning can be compute-intensive depending on model choice and input size.
