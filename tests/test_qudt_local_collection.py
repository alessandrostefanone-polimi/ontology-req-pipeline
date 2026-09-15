from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from ontology_req_pipeline.normalization import qudt_normalization


class FakeCollection:
    def __init__(self) -> None:
        self.rows: dict[str, tuple[str, dict[str, Any]]] = {}
        self.upsert_calls = 0

    def count(self) -> int:
        return len(self.rows)

    def upsert(self, *, ids, documents, metadatas) -> None:
        self.upsert_calls += 1
        for item_id, document, metadata in zip(ids, documents, metadatas, strict=True):
            self.rows[item_id] = (document, metadata)


class FakePersistentClient:
    def __init__(self) -> None:
        self.collections: dict[str, FakeCollection] = {}

    def get_or_create_collection(self, *, name: str, metadata: dict[str, Any]) -> FakeCollection:
        del metadata
        return self.collections.setdefault(name, FakeCollection())

    def delete_collection(self, name: str) -> None:
        self.collections.pop(name, None)


@pytest.fixture(autouse=True)
def clear_collection_cache() -> None:
    qudt_normalization._COLLECTION_CACHE.clear()
    yield
    qudt_normalization._COLLECTION_CACHE.clear()


def _write_test_csv(path: Path) -> None:
    path.write_text(
        "quantity_kind,unit_uri,unit,symbol,dimVec,description,description_dcterms,label,comment,quantity_name\n"
        "http://qudt.org/vocab/quantitykind/Pressure,http://qudt.org/vocab/unit/PA,PA,Pa,D1,Pressure,,Pressure,,Pressure\n"
        "http://qudt.org/vocab/quantitykind/Pressure,http://qudt.org/vocab/unit/BAR,BAR,bar,D1,Pressure,,Pressure,,Pressure\n"
        "http://qudt.org/vocab/quantitykind/Length,http://qudt.org/vocab/unit/M,M,m,D2,Length,,Length,,Length\n",
        encoding="utf-8",
    )


def test_local_collection_is_built_from_bundled_csv_and_reused(tmp_path, monkeypatch) -> None:
    csv_path = tmp_path / "qudt.csv"
    cache_path = tmp_path / "chroma"
    _write_test_csv(csv_path)
    client = FakePersistentClient()
    monkeypatch.setattr(qudt_normalization, "QUDT_LOOKUP_CSV", csv_path)
    monkeypatch.setattr(qudt_normalization.chromadb, "PersistentClient", lambda **kwargs: client)
    monkeypatch.setenv("QUDT_CHROMA_BACKEND", "local")
    monkeypatch.setenv("QUDT_CHROMA_PATH", str(cache_path))
    monkeypatch.delenv("CHROMA_API_KEY", raising=False)
    monkeypatch.delenv("CHROMA_TENANT", raising=False)
    monkeypatch.delenv("CHROMA_DATABASE", raising=False)

    first = qudt_normalization._load_qudt_collection()
    qudt_normalization._COLLECTION_CACHE.clear()
    second = qudt_normalization._load_qudt_collection()

    assert first is second
    assert first.count() == 2
    assert first.upsert_calls == 1
    assert {metadata["quantity_kind_uri"] for _, metadata in first.rows.values()} == {
        "http://qudt.org/vocab/quantitykind/Length",
        "http://qudt.org/vocab/quantitykind/Pressure",
    }
    manifests = list(cache_path.glob("*.qudt-index.json"))
    assert len(manifests) == 1
    assert json.loads(manifests[0].read_text(encoding="utf-8"))["document_count"] == 2


def test_local_collection_failure_falls_back_without_cloud_credentials(tmp_path, monkeypatch) -> None:
    csv_path = tmp_path / "qudt.csv"
    _write_test_csv(csv_path)
    monkeypatch.setattr(qudt_normalization, "QUDT_LOOKUP_CSV", csv_path)
    monkeypatch.setattr(
        qudt_normalization.chromadb,
        "PersistentClient",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("embedding unavailable")),
    )
    monkeypatch.setenv("QUDT_CHROMA_BACKEND", "local")
    monkeypatch.setenv("QUDT_CHROMA_PATH", str(tmp_path / "cache"))

    with pytest.warns(RuntimeWarning, match="Deterministic RDF/CSV normalization will continue"):
        assert qudt_normalization._load_qudt_collection() is None


def test_explicit_cloud_backend_requires_cloud_credentials(monkeypatch) -> None:
    monkeypatch.setenv("QUDT_CHROMA_BACKEND", "cloud")
    monkeypatch.delenv("CHROMA_API_KEY", raising=False)
    monkeypatch.delenv("CHROMA_TENANT", raising=False)
    monkeypatch.delenv("CHROMA_DATABASE", raising=False)

    with pytest.raises(OSError, match="CHROMA_API_KEY, CHROMA_TENANT, CHROMA_DATABASE"):
        qudt_normalization._load_qudt_collection()
