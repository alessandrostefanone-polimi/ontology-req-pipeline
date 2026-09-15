from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import BaseModel

from ontology_req_pipeline.extraction.utils import (
    OllamaStructuredResponseError,
    run_ollama,
)
from ontology_req_pipeline.normalization.utils import _run_structured_response
from ontology_req_pipeline.ontology.agentic_kg_builder import AgenticKGBuilder


class ExampleResponse(BaseModel):
    value: str


class FakeNativeOllamaClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def chat(self, **kwargs: Any):
        self.calls.append(kwargs)
        return SimpleNamespace(
            message=SimpleNamespace(content=json.dumps({"value": "ok"}), thinking=None),
            prompt_eval_count=3,
            eval_count=2,
        )


def test_extraction_ollama_call_disables_thinking() -> None:
    client = FakeNativeOllamaClient()

    result = run_ollama(
        client, "prompt", output_model=ExampleResponse, model="local-model"
    )

    assert result.value == "ok"
    assert client.calls[0]["think"] is False


class FakeInvalidOllamaClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def chat(self, **kwargs: Any):
        self.calls.append(kwargs)
        if len(self.calls) == 1:
            content = '{"value":'
            done_reason = "length"
        else:
            content = json.dumps({"wrong_field": "missing required value"})
            done_reason = "stop"
        return SimpleNamespace(
            message=SimpleNamespace(content=content, thinking=None),
            done=True,
            done_reason=done_reason,
            prompt_eval_count=123,
            eval_count=len(content),
        )


def test_extraction_ollama_failure_preserves_attempt_diagnostics() -> None:
    client = FakeInvalidOllamaClient()

    with pytest.raises(OllamaStructuredResponseError) as caught:
        run_ollama(client, "prompt", output_model=ExampleResponse, model="local-model")

    diagnostics = caught.value.provider_diagnostics
    assert len(diagnostics) == 2
    assert diagnostics[0]["error_type"] == "JSONDecodeError"
    assert diagnostics[0]["done_reason"] == "length"
    assert diagnostics[0]["raw_response"] == '{"value":'
    assert diagnostics[0]["raw_response_length"] == len('{"value":')
    assert diagnostics[0]["raw_response_truncated"] is False
    assert diagnostics[1]["error_type"] == "ValidationError"
    assert diagnostics[1]["done_reason"] == "stop"
    assert diagnostics[1]["prompt_eval_count"] == 123
    assert len(client.calls) == 2
    assert all(call["think"] is False for call in client.calls)
    repair_message = client.calls[1]["messages"][-1]
    assert repair_message["role"] == "user"
    assert "Validation feedback: JSONDecodeError" in repair_message["content"]
    assert "length limit" in repair_message["content"]


def test_normalization_ollama_call_disables_thinking() -> None:
    client = FakeNativeOllamaClient()

    result = _run_structured_response(
        client,
        provider="ollama",
        model="local-model",
        system_prompt="system",
        user_prompt="user",
        output_model=ExampleResponse,
    )

    assert result.value == "ok"
    assert client.calls[0]["think"] is False


def test_grounding_uses_native_ollama_with_thinking_disabled() -> None:
    client = FakeNativeOllamaClient()
    builder = AgenticKGBuilder.__new__(AgenticKGBuilder)
    builder.llm_provider = "ollama"
    builder.client = client
    builder.ollama_think = False
    builder.prompt_cache_enabled = False
    builder.last_prompt_cache_usage = None

    completion = builder._chat_completion_create(
        model="local-model",
        temperature=0,
        messages=[{"role": "user", "content": "prompt"}],
    )

    assert client.calls[0]["think"] is False
    assert client.calls[0]["options"]["temperature"] == 0
    assert builder._completion_content(completion) == json.dumps({"value": "ok"})
    assert builder.last_prompt_cache_usage == {
        "prompt_tokens": 3,
        "completion_tokens": 2,
        "total_tokens": 5,
        "cached_tokens": None,
    }


def test_grounding_can_enable_thinking_for_native_ollama() -> None:
    client = FakeNativeOllamaClient()
    builder = AgenticKGBuilder.__new__(AgenticKGBuilder)
    builder.llm_provider = "ollama"
    builder.client = client
    builder.ollama_think = True
    builder.prompt_cache_enabled = False
    builder.last_prompt_cache_usage = None

    builder._chat_completion_create(
        model="local-model",
        temperature=0,
        messages=[{"role": "user", "content": "prompt"}],
    )

    assert client.calls[0]["think"] is True
