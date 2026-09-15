from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import httpx
import pytest
from openai import BadRequestError

from ontology_req_pipeline.ontology.agentic_kg_builder import AgenticKGBuilder


def _bad_request(*, param: str, code: str, message: str) -> BadRequestError:
    request = httpx.Request("POST", "https://api.openai.com/v1/chat/completions")
    response = httpx.Response(400, request=request)
    return BadRequestError(
        message,
        response=response,
        body={
            "error": {
                "message": message,
                "type": "invalid_request_error",
                "param": param,
                "code": code,
            }
        },
    )


def _completion() -> SimpleNamespace:
    return SimpleNamespace(
        choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))],
        usage=None,
    )


class TemperatureRejectingCompletions:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> SimpleNamespace:
        self.calls.append(kwargs.copy())
        if len(self.calls) == 1:
            raise _bad_request(
                param="temperature",
                code="unsupported_value",
                message=(
                    "Unsupported value: 'temperature' does not support 0 with this model. "
                    "Only the default value is supported."
                ),
            )
        return _completion()


class AlwaysFailingCompletions:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    def create(self, **kwargs: Any) -> SimpleNamespace:
        self.calls.append(kwargs.copy())
        raise _bad_request(
            param="messages",
            code="invalid_value",
            message="Invalid messages payload.",
        )


def _builder_with(completions: Any) -> AgenticKGBuilder:
    builder = AgenticKGBuilder.__new__(AgenticKGBuilder)
    builder.llm_provider = "openai"
    builder.client = SimpleNamespace(chat=SimpleNamespace(completions=completions))
    builder.prompt_cache_enabled = False
    builder._models_without_temperature = set()
    builder.last_prompt_cache_usage = None
    return builder


def test_openai_retries_without_unsupported_temperature_and_remembers_model() -> None:
    completions = TemperatureRejectingCompletions()
    builder = _builder_with(completions)

    result = builder._chat_completion_create(
        model="gpt-5.6-luna",
        temperature=0,
        messages=[{"role": "user", "content": "prompt"}],
    )
    builder._chat_completion_create(
        model="gpt-5.6-luna",
        temperature=0,
        messages=[{"role": "user", "content": "another prompt"}],
    )

    assert AgenticKGBuilder._completion_content(result) == "ok"
    assert completions.calls[0]["temperature"] == 0
    assert "temperature" not in completions.calls[1]
    assert "temperature" not in completions.calls[2]


def test_openai_does_not_retry_unrelated_bad_requests() -> None:
    completions = AlwaysFailingCompletions()
    builder = _builder_with(completions)

    with pytest.raises(BadRequestError, match="Invalid messages payload"):
        builder._chat_completion_create(
            model="gpt-5.6-luna",
            temperature=0,
            messages=[{"role": "user", "content": "prompt"}],
        )

    assert len(completions.calls) == 1
