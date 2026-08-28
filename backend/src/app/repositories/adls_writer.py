from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path, PurePosixPath
from threading import Lock
from typing import Any
from urllib.parse import quote
from uuid import uuid4

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient

from app.models import BinaryFileWrite, PageWrite
from app.repositories.writer import (
    StorageWriteResult,
    merge_file_manifest,
    merge_page_manifest,
    payload_sha256,
)

logger = logging.getLogger(__name__)


class ADLSGen2BronzeWriter:
    """Bronze writer for an ADLS Gen2 account with hierarchical namespace."""

    def __init__(
        self,
        account_name: str,
        file_system: str,
        base_path: str = "",
    ) -> None:
        normalized_account = account_name.strip()
        normalized_file_system = file_system.strip()
        if not normalized_account:
            raise ValueError("ADLS_ACCOUNT_NAME is required for ADLS Bronze storage")
        if not normalized_file_system:
            raise ValueError("ADLS_FILE_SYSTEM is required for ADLS Bronze storage")

        self.account_name = normalized_account
        self.file_system = normalized_file_system
        self.base_path = self._normalize_base_path(base_path)
        self._credential = DefaultAzureCredential()
        self._service_client = DataLakeServiceClient(
            account_url=f"https://{self.account_name}.dfs.core.windows.net",
            credential=self._credential,
        )
        self._file_system_client: FileSystemClient = (
            self._service_client.get_file_system_client(self.file_system)
        )
        self._manifest_lock = Lock()

    async def write_page(self, page: PageWrite) -> StorageWriteResult:
        return await asyncio.to_thread(self._write_page, page)

    async def write_file(self, file: BinaryFileWrite) -> StorageWriteResult:
        return await asyncio.to_thread(self._write_file, file)

    def _write_page(self, page: PageWrite) -> StorageWriteResult:
        if not page.raw_payload:
            raise ValueError("Refusing to write an empty raw payload")

        directory = self._run_directory(
            page.vendor,
            page.data_domain,
            page.ingestion_date,
            page.run_id,
        )
        if page.course_id is not None:
            directory = f"{directory}/course_id={quote(page.course_id, safe='-_.')}"
        output_path = f"{directory}/offset={page.offset:06d}.json"
        sha256 = payload_sha256(page.raw_payload)
        uri = self._upload_atomically(output_path, page.raw_payload, sha256)

        manifest_path = f"{directory}/manifest.json"
        with self._manifest_lock:
            manifest = merge_page_manifest(
                self._load_manifest(manifest_path),
                page,
                file_name=PurePosixPath(output_path).name,
                sha256=sha256,
            )
            self._upload_manifest(manifest_path, manifest)

        logger.debug(
            "Bronze page stored vendor=%s domain=%s run_id=%s offset=%d "
            "records_count=%d payload_bytes=%d file=%s",
            page.vendor,
            page.data_domain,
            page.run_id,
            page.offset,
            page.records_count,
            len(page.raw_payload),
            PurePosixPath(output_path).name,
        )
        return StorageWriteResult(
            uri=uri,
            size_bytes=len(page.raw_payload),
            sha256=sha256,
        )

    def _write_file(self, file: BinaryFileWrite) -> StorageWriteResult:
        if not file.raw_payload:
            raise ValueError("Refusing to write an empty raw payload")
        if Path(file.file_name).name != file.file_name:
            raise ValueError("Binary Bronze file_name must not contain a path")
        if file.file_size != len(file.raw_payload):
            raise ValueError("Binary Bronze file size does not match payload")

        directory = self._run_directory(
            file.vendor,
            file.data_domain,
            file.ingestion_date,
            file.run_id,
        )
        output_path = f"{directory}/{file.file_name}"
        sha256 = payload_sha256(file.raw_payload)
        uri = self._upload_atomically(output_path, file.raw_payload, sha256)

        manifest_path = f"{directory}/manifest.json"
        with self._manifest_lock:
            manifest = merge_file_manifest(
                self._load_manifest(manifest_path),
                file,
                sha256=sha256,
            )
            self._upload_manifest(manifest_path, manifest)

        logger.debug(
            "Bronze file stored vendor=%s domain=%s run_id=%s "
            "records_count=%d payload_bytes=%d file=%s",
            file.vendor,
            file.data_domain,
            file.run_id,
            file.records_count,
            len(file.raw_payload),
            file.file_name,
        )
        return StorageWriteResult(
            uri=uri,
            size_bytes=len(file.raw_payload),
            sha256=sha256,
        )

    def _run_directory(
        self,
        vendor: str,
        data_domain: str,
        ingestion_date: str,
        run_id: str,
    ) -> str:
        relative = (
            f"{vendor}/{data_domain}/ingestion_date={ingestion_date}/"
            f"run_id={quote(run_id, safe='-_')}"
        )
        return f"{self.base_path}/{relative}" if self.base_path else relative

    def _upload_atomically(self, final_path: str, payload: bytes, sha256: str) -> str:
        self._ensure_directory(str(PurePosixPath(final_path).parent))
        temporary_path = f"{final_path}.tmp-{uuid4().hex}"
        temporary_client = self._file_system_client.get_file_client(temporary_path)
        final_client = self._file_system_client.get_file_client(final_path)
        temporary_client.upload_data(payload, overwrite=False)
        try:
            if final_client.exists():
                existing = final_client.download_file().readall()
                if payload_sha256(existing) == sha256:
                    temporary_client.delete_file()
                    return str(final_client.url)
                final_client.delete_file()
            renamed = temporary_client.rename_file(
                f"{self.file_system}/{final_path}"
            )
            return str(renamed.url)
        except BaseException:
            try:
                temporary_client.delete_file()
            except ResourceNotFoundError:
                pass
            raise

    def _ensure_directory(self, directory_path: str) -> None:
        current: list[str] = []
        for part in PurePosixPath(directory_path).parts:
            current.append(part)
            directory = self._file_system_client.get_directory_client("/".join(current))
            try:
                directory.create_directory()
            except ResourceExistsError:
                pass

    def _load_manifest(self, path: str) -> dict[str, Any]:
        try:
            payload = self._file_system_client.get_file_client(path).download_file().readall()
            value = json.loads(payload)
        except (ResourceNotFoundError, json.JSONDecodeError, UnicodeDecodeError):
            return {}
        return (
            {str(key): item for key, item in value.items()}
            if isinstance(value, dict)
            else {}
        )

    def _upload_manifest(self, path: str, manifest: dict[str, Any]) -> None:
        payload = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")
        self._file_system_client.get_file_client(path).upload_data(
            payload,
            overwrite=True,
        )

    @staticmethod
    def _normalize_base_path(base_path: str) -> str:
        normalized = base_path.strip().strip("/")
        if not normalized:
            return ""
        parts = PurePosixPath(normalized).parts
        if any(part in {".", ".."} for part in parts):
            raise ValueError("ADLS_BASE_PATH must not contain relative path segments")
        return "/".join(parts)
