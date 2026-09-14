from __future__ import annotations

import asyncio

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import ContainerClient
from azure.storage.queue import QueueClient, TextBase64EncodePolicy

from app.services.ingestion_coordinator import (
    IngestionState,
    QueuedIngestion,
)


class AzureIngestionBackend:
    """Queue manual jobs and persist their status in Azure Blob Storage."""

    def __init__(
        self,
        connection_string: str,
        queue_name: str,
        status_container: str,
    ) -> None:
        self._queue = QueueClient.from_connection_string(
            connection_string,
            queue_name,
            message_encode_policy=TextBase64EncodePolicy(),
        )
        self._container = ContainerClient.from_connection_string(
            connection_string,
            status_container,
        )
        self._ready = False
        self._ready_lock = asyncio.Lock()

    async def create(self, state: IngestionState) -> None:
        await self._ensure_resources()
        await asyncio.to_thread(
            self._container.upload_blob,
            self._blob_name(state.job_id),
            state.model_dump_json(),
            overwrite=False,
        )

    async def save(self, state: IngestionState) -> None:
        await self._ensure_resources()
        await asyncio.to_thread(
            self._container.upload_blob,
            self._blob_name(state.job_id),
            state.model_dump_json(),
            overwrite=True,
        )

    async def get(self, job_id: str) -> IngestionState | None:
        await self._ensure_resources()
        try:
            payload = await asyncio.to_thread(
                lambda: self._container.download_blob(self._blob_name(job_id)).readall()
            )
        except ResourceNotFoundError:
            return None
        return IngestionState.model_validate_json(payload)

    async def enqueue(self, message: QueuedIngestion) -> None:
        await self._ensure_resources()
        await asyncio.to_thread(self._queue.send_message, message.model_dump_json())

    async def _ensure_resources(self) -> None:
        if self._ready:
            return
        async with self._ready_lock:
            if self._ready:
                return
            for create in (self._queue.create_queue, self._container.create_container):
                try:
                    await asyncio.to_thread(create)
                except ResourceExistsError:
                    pass
            self._ready = True

    @staticmethod
    def _blob_name(job_id: str) -> str:
        return f"manual-ingestions/{job_id}.json"
