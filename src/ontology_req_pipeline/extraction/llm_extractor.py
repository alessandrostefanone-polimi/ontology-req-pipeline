"""LLM-based extractor for structured requirement extraction."""

from __future__ import annotations

from ollama import Client
from openai import OpenAI
from typing import Any, Optional

from ontology_req_pipeline.extraction.extractor_base import BaseExtractor
from ontology_req_pipeline.extraction.utils import process_text

EXAMPLE_SENTENCE = (
    "Fastened connections between vertical walls around sections containing between 8 kg and 12 kg "
    "must have a minimum of three fasteners"
)


class LLMExtractor(BaseExtractor):
    """Extractor backed by either OpenAI or Ollama."""

    def extract(
        self,
        text: str,
        local=False,
        idx: Optional[str] = "demo",
        model: Optional[str] = None,
        prompt_style: str = "few_shot",
    ) -> Any:
        """Extract structured requirement information from raw text."""

        if local:
            client = Client()
        else:
            client = OpenAI()

        selected_model = model if model else ("llama3.2" if local else "gpt-5.1")
        response = process_text(
            client,
            text,
            idx,
            local=local,
            model=selected_model,
            prompt_style=prompt_style,
        )

        return response


def get_default_extractor() -> BaseExtractor:
    """Return the default extractor implementation."""
    return LLMExtractor()
