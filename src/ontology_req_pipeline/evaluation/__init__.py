"""Evaluation utilities."""

from __future__ import annotations

from typing import Any, Dict


def evaluate_results(*args: Any, **kwargs: Any) -> Dict[str, Any]:
    """Lazy proxy to avoid importing metrics at package import time."""
    from .metrics import evaluate_results as _evaluate_results

    return _evaluate_results(*args, **kwargs)


__all__ = ["evaluate_results"]
