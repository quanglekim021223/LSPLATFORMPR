from __future__ import annotations

from datetime import UTC, datetime

from azure.core.exceptions import ResourceNotFoundError

from app.services.azure_ingestion_backend import AzureIngestionBackend
from app.services.ingestion_coordinator import (
    IngestionState,
    QueuedIngestion,
    VendorIngestionState,
)


class FakeDownload:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload

    def readall(self) -> bytes:
        return self.payload


class FakeContainer:
    def __init__(self) -> None:
        self.blobs: dict[str, bytes] = {}
        self.created = False

    def create_container(self) -> None:
        self.created = True

    def upload_blob(self, name: str, payload: str, *, overwrite: bool) -> None:
        if not overwrite and name in self.blobs:
            raise AssertionError("test attempted to overwrite durable job state")
        self.blobs[name] = payload.encode()

    def download_blob(self, name: str) -> FakeDownload:
        try:
            return FakeDownload(self.blobs[name])
        except KeyError as exc:
            raise ResourceNotFoundError("missing test blob") from exc


class FakeQueue:
    def __init__(self) -> None:
        self.messages: list[str] = []
        self.created = False

    def create_queue(self) -> None:
        self.created = True

    def send_message(self, payload: str) -> None:
        self.messages.append(payload)


async def test_azure_backend_persists_status_and_enqueues_message(monkeypatch) -> None:
    queue = FakeQueue()
    container = FakeContainer()
    monkeypatch.setattr(
        "app.services.azure_ingestion_backend.QueueClient.from_connection_string",
        lambda *_args, **_kwargs: queue,
    )
    monkeypatch.setattr(
        "app.services.azure_ingestion_backend.ContainerClient.from_connection_string",
        lambda *_args, **_kwargs: container,
    )
    backend = AzureIngestionBackend("test-connection", "jobs", "statuses")
    state = IngestionState(
        job_id="job-1",
        created_at=datetime.now(UTC),
        vendor_runs=[VendorIngestionState(vendor="skillup")],
    )
    message = QueuedIngestion(job_id=state.job_id, vendors=["skillup"])

    await backend.create(state)
    await backend.enqueue(message)
    restored = await backend.get(state.job_id)

    assert queue.created is True
    assert container.created is True
    assert restored == state
    assert QueuedIngestion.model_validate_json(queue.messages[0]) == message
    assert await backend.get("missing") is None
