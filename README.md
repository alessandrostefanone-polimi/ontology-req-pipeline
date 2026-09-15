# Ontology-Requirements Pipeline

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
- `datasets/`: curated public datasets for the published examples and evaluation flow.

## Public Release Notes

- The public repository keeps the selected evaluation inputs in `datasets/` and the published reference runs in `src/ontology_req_pipeline/evaluation/`.
- Larger extracted corpora and intermediate research datasets are intentionally excluded from the public repo.
- Default CLI examples point to `datasets/fsae_test_number_unit_sample.jsonl`.
- The ablation-study input used for `techreq_no_fsae_comparison` is published as `datasets/techreq_no_fsae.jsonl`.
- Generated RDF/OWL/HTML outputs under `src/ontology_req_pipeline/outputs/` are excluded from version control.
- Third-party licenses and attribution notes are listed in `THIRD_PARTY_NOTICES.md`.

## Requirements

- Python 3.10.13+
- JDK 17 for the default Owlapy/Pellet reasoning backend, with `JAVA_HOME` set
  to the JDK installation directory
- For OpenAI provider: `OPENAI_API_KEY`
- For Ollama provider: running Ollama server (default URL `http://localhost:11434/v1`)
- Pellet reasoning backend is used by default via Owlapy (`--reasoner Pellet`)

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\activate
pip install -e .
```

On Windows, configure the JDK for the current PowerShell session before running
the grounding stage (adjust the path if your JDK is installed elsewhere):

```powershell
$env:JAVA_HOME = "C:\Program Files\Java\jdk-17.0.20"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
java -version
```

`java -version` must resolve to JDK 17 rather than an older Java installation.
The dependency set pins JPype below 1.7 because JPype 1.7.1 cannot enumerate
Owlapy 1.5.1's bundled JARs on Windows/JDK 17.

If you prefer a requirements-based install:

```powershell
pip install -r requirements.txt
pip install -e .
```

Install development dependencies (including `pytest`):

```powershell
pip install -e ".[dev]"
```

Install PDF ingestion and the local review interface:

```powershell
pip install -e ".[document,ui]"
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
- `DOCUMENT_OLLAMA_NUM_CTX` (default `8192` for PDF extraction)
- `DOCUMENT_OLLAMA_NUM_PREDICT` (default `3072` for PDF extraction)
- `DOCUMENT_PROMPT_TOKEN_BUDGET` (default `4096`, including instructions and JSON schema)
- `DOCUMENT_LOW_BATCH_MAX_TOKENS` (default `3072`)

All native Ollama requests explicitly set `think=False`, including PDF discovery,
structured extraction, and QUDT normalization. Grounding uses the native Ollama
client because the OpenAI-compatible endpoint does not reliably honor the thinking
flag for every model. The UI can optionally enable thinking for Ollama grounding and
graph-repair calls only; OpenAI grounding and the earlier stages are unaffected.

QUDT semantic lookup uses a local persistent Chroma index by default:
- `QUDT_CHROMA_BACKEND` (`local` by default; also accepts `cloud`, `auto`, or `disabled`)
- `QUDT_CHROMA_PATH` (default `artifacts/cache/qudt-chroma`)
- `CHROMA_COLLECTION` (default `qudt_quantity_kinds_with_descriptions`)

The local index is generated automatically from the bundled QUDT CSV on first use and
reused on later runs. It is a disposable cache under `artifacts/` and should not be
committed. If the embedding model cannot initialize, deterministic RDF/CSV normalization
continues without semantic search.

Optional Chroma Cloud backend (`QUDT_CHROMA_BACKEND=cloud`):
- `CHROMA_API_KEY`
- `CHROMA_TENANT`
- `CHROMA_DATABASE`

`auto` uses Cloud when all three credentials are present and otherwise uses the local index.

## CLI Usage

Main entry point:

```powershell
python -m ontology_req_pipeline.cli --help
```

## PDF-to-Ontology Workflow

The document workflow is resumable and stores every run under `artifacts/runs/<run-id>/`.
It preserves the source PDF, Docling chunks, evidence spans, routing decisions, model
outputs, human decisions, lineage, downstream pipeline artifacts, and QA reports.

### Local interface

Start the interface with either command:

```powershell
ontology-req-pipeline ui
```

```powershell
ontology-req-pipeline-ui
```

The interface provides five stages:

1. Upload a PDF, select OpenAI or Ollama, configure scoring/concurrency, and run extraction.
2. Review the prioritized source queue alongside rendered PDF pages and grounded evidence spans.
3. Approve, edit, or reject only the requirements affected by HITL recovery.
4. Select the machine, HITL-final, or adjudicated requirement set and run the ontology pipeline.
5. Inspect metrics and download JSONL, reports, and RDF/OWL artifacts.

The first downstream run defaults to a small requirement limit because extraction,
normalization, ontology grounding, and reasoning may each invoke external models or tools.

### Command-line workflow

Create a PDF extraction run:

```powershell
ontology-req-pipeline extract-pdf `
  --pdf path/to/document.pdf `
  --output-root artifacts/runs `
  --provider ollama `
  --model qwen3.5:9b-bf16 `
  --num-ctx 8192 `
  --num-predict 3072 `
  --prompt-token-budget 4096 `
  --low-batch-max-tokens 3072 `
  --max-chunks 5
```

Native Ollama document calls disable thinking, set the context and output ceilings explicitly,
and reject estimated prompts that exceed the configured budget. Low-confidence batches are
formed from the rendered prompt plus structured-output schema rather than source character count.
Over-budget calls and rejected evidence-grounding candidates are retained in `logs/llm_debug.jsonl`
for review instead of being sent or discarded silently.

The command creates a machine baseline and a review queue. Review decisions can be
entered through the UI. They are stored as current-state JSONL plus append-only audit events.

Apply saved decisions and prepare focused adjudication:

```powershell
ontology-req-pipeline apply-document-review `
  --run-dir artifacts/runs/<run-id>
```

After the focused UI review, finalize it:

```powershell
ontology-req-pipeline finalize-document-adjudication `
  --run-dir artifacts/runs/<run-id>
```

Export a pipeline-compatible JSONL file without running downstream stages:

```powershell
ontology-req-pipeline export-document-requirements `
  --run-dir artifacts/runs/<run-id> `
  --source best
```

Run the complete downstream pipeline from the reviewed PDF requirements:

```powershell
ontology-req-pipeline run-document-pipeline `
  --run-dir artifacts/runs/<run-id> `
  --source adjudicated `
  --limit 3 `
  --provider openai `
  --model gpt-5.1
```

`--source best` selects `adjudicated`, then `final`, then `machine`, using the most
advanced artifact available.

### Run artifact layout

```text
artifacts/runs/<run-id>/
  source/                 copied source PDF
  document_extraction/    chunks, triage, and machine candidates
  hitl/                   queues, decisions, lineage, and reviewed requirements
  pipeline/               adapted JSONL and downstream evaluation outputs
  logs/                   structured model/debug events
  run_manifest.json       configuration, status, counts, errors, and artifact paths
```

Requirement rows passed downstream include `idx`, `original_text`, and extended source
metadata: document hash, page, chunk, evidence IDs, requirement fingerprint, origin,
and reviewer. That lineage is retained in extraction and normalization records.

### 1) Run a Single Demo Pipeline

```powershell
python -m ontology_req_pipeline.cli run-pipeline
```

`run-pipeline` is a fixed smoke-test flow with an internal example sentence.

### 2) Generate a Labeled Extraction Dataset

```powershell
python -m ontology_req_pipeline.cli generate-labeled-dataset `
  --input-path datasets/fsae_test_number_unit_sample.jsonl `
  --output-path src/ontology_req_pipeline/evaluation/labeled_dataset.jsonl `
  --limit 10 `
  --provider openai `
  --model gpt-5.1
```

### 3) Run Full Batch Evaluation Pipeline

```powershell
python -m ontology_req_pipeline.cli run-evaluation-pipeline `
  --input-path datasets/fsae_test_number_unit_sample.jsonl `
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
  --output-dir src/ontology_req_pipeline/evaluation/fsae_test_pipeline_rerun
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

- Large source corpora and intermediate extracted datasets are intentionally omitted from the public repo.
- Grounding/reasoning can be compute-intensive depending on model choice and input size.
