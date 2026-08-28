from __future__ import annotations

from pathlib import Path
from typing import Any

from app.core.config import Settings
from app.main import build_bronze_writer
from app.repositories import LocalBronzeWriter


def test_build_bronze_writer_selects_local_storage(
    settings_factory: Any,
    tmp_path: Path,
) -> None:
    settings: Settings = settings_factory(
        bronze_storage_type="local",
        bronze_local_path=tmp_path / "bronze",
    )

    writer = build_bronze_writer(settings)

    assert isinstance(writer, LocalBronzeWriter)
    assert writer.root == tmp_path / "bronze"


def test_build_bronze_writer_selects_adls_storage(
    settings_factory: Any,
    monkeypatch: Any,
) -> None:
    captured: dict[str, str] = {}
    sentinel = object()

    def build_adls_writer(**kwargs: str) -> object:
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr("app.main.ADLSGen2BronzeWriter", build_adls_writer)
    settings: Settings = settings_factory(
        bronze_storage_type="adls",
        adls_account_name="fsaaccount",
        adls_file_system="bronze",
        adls_base_path="raw",
    )

    writer = build_bronze_writer(settings)

    assert writer is sentinel
    assert captured == {
        "account_name": "fsaaccount",
        "file_system": "bronze",
        "base_path": "raw",
    }
