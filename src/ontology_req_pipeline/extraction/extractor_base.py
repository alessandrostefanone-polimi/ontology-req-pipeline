"""Base interface for requirement extraction engines."""

from __future__ import annotations
from abc import ABC, abstractmethod


class BaseExtractor(ABC):
    """Abstract extractor API."""

    @abstractmethod
    def extract(self, text: str):
        """Extract structured requirement information from raw text."""
        raise NotImplementedError

        
        
