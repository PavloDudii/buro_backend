from collections.abc import Iterator, Sequence
from dataclasses import dataclass
import hashlib
from datetime import datetime, timedelta, timezone
from io import BytesIO
from uuid import UUID
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from httpx import AsyncClient
from sqlalchemy import select

from src.core.db.session import AsyncSessionLocal
from src.core.dependencies import get_blob_storage, get_document_processing_scheduler
from src.core.security import utcnow
from src.main import app
from src.models.document import (
    DocumentExtractionItem,
    DocumentProcessingRun,
    DocumentProcessingStatus,
    DocumentUploadIntent,
    DocumentUploadIntentStatus,
    UploadedDocument,
)
from src.services.blob_storage import StoredBlob, build_document_blob_path
from src.services.document_upload import MAX_UPLOAD_FILE_SIZE_BYTES
from src.tests.test_auth import register_user


@dataclass(frozen=True)
class BlobPutCall:
    user_id: UUID
    document_id: UUID
    safe_filename: str
    content: bytes
    content_type: str
    uploaded_at: datetime


class FakeBlobStorage:
    def __init__(self) -> None:
        self.calls: list[BlobPutCall] = []
        self.objects: dict[str, bytes] = {}
        self.deleted_pathnames: list[str] = []
        self.fail_on_call: int | None = None

    async def put_document(
        self,
        *,
        user_id: UUID,
        document_id: UUID,
        safe_filename: str,
        content: bytes,
        content_type: str,
        uploaded_at: datetime,
    ) -> StoredBlob:
        call = BlobPutCall(
            user_id=user_id,
            document_id=document_id,
            safe_filename=safe_filename,
            content=content,
            content_type=content_type,
            uploaded_at=uploaded_at,
        )
        self.calls.append(call)
        if self.fail_on_call == len(self.calls):
            raise RuntimeError("blob upload failed")

        pathname = build_document_blob_path(
            user_id=user_id,
            document_id=document_id,
            safe_filename=safe_filename,
            uploaded_at=uploaded_at,
        )
        self.objects[pathname] = content
        return StoredBlob(
            pathname=pathname,
            url=f"https://blob.test/{pathname}",
            download_url=f"https://blob.test/{pathname}?download=1",
            etag=f"etag-{len(self.calls)}",
        )

    async def delete_documents(self, pathnames: Sequence[str]) -> None:
        self.deleted_pathnames.extend(pathnames)

    async def get_document_content(self, pathname: str) -> bytes:
        return self.objects[pathname]


class FakeProcessingScheduler:
    def __init__(self) -> None:
        self.scheduled_document_ids: list[UUID] = []

    def schedule_documents(self, document_ids: Sequence[UUID]) -> None:
        self.scheduled_document_ids.extend(document_ids)


@pytest.fixture(autouse=True)
def blob_storage_override() -> Iterator[FakeBlobStorage]:
    fake_blob_storage = FakeBlobStorage()
    app.dependency_overrides[get_blob_storage] = lambda: fake_blob_storage
    yield fake_blob_storage
    app.dependency_overrides.pop(get_blob_storage, None)


@pytest.fixture(autouse=True)
def processing_scheduler_override() -> Iterator[FakeProcessingScheduler]:
    scheduler = FakeProcessingScheduler()
    app.dependency_overrides[get_document_processing_scheduler] = lambda: scheduler
    yield scheduler
    app.dependency_overrides.pop(get_document_processing_scheduler, None)


def pdf_bytes() -> bytes:
    return b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj\n%%EOF\n"


def pdf_with_font_name_containing_aa_bytes() -> bytes:
    return b"%PDF-1.4\n1 0 obj\n<< /FontName /AAAAAA+Lora-Regular >>\nendobj\n%%EOF\n"


def docx_bytes() -> bytes:
    buffer = BytesIO()
    with ZipFile(buffer, "w", ZIP_DEFLATED) as archive:
        archive.writestr(
            "[Content_Types].xml",
            (
                '<?xml version="1.0" encoding="UTF-8"?>'
                '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                '<Default Extension="xml" ContentType="application/xml"/>'
                "</Types>"
            ),
        )
        archive.writestr("word/document.xml", "<w:document />")
    return buffer.getvalue()


async def admin_token(client: AsyncClient) -> str:
    response = await client.post(
        "/api/v1/auth/login",
        json={"email": "admin@buro.com", "password": "AdminPassword123!"},
    )
    assert response.status_code == 200
    return response.json()["access_token"]


async def upload_pdf(client: AsyncClient, *, filename: str = "policy.pdf") -> dict:
    token = await admin_token(client)
    response = await client.post(
        "/api/v1/documents/uploads",
        headers={"Authorization": f"Bearer {token}"},
        files=[("files", (filename, pdf_bytes(), "application/pdf"))],
    )
    assert response.status_code == 201, response.text
    return response.json()["items"][0]


def test_build_document_blob_path_uses_user_date_id_and_filename() -> None:
    user_id = UUID("11111111-1111-1111-1111-111111111111")
    document_id = UUID("22222222-2222-2222-2222-222222222222")
    uploaded_at = datetime(2026, 5, 2, 12, 30, tzinfo=timezone.utc)

    assert (
        build_document_blob_path(
            user_id=user_id,
            document_id=document_id,
            safe_filename="Course_Plan.pdf",
            uploaded_at=uploaded_at,
        )
        == "documents/11111111-1111-1111-1111-111111111111/2026/05/"
        "22222222-2222-2222-2222-222222222222-Course_Plan.pdf"
    )


@pytest.mark.anyio
async def test_admin_upload_saves_file_to_blob_storage(
    client: AsyncClient,
    blob_storage_override: FakeBlobStorage,
    processing_scheduler_override: FakeProcessingScheduler,
) -> None:
    token = await admin_token(client)

    response = await client.post(
        "/api/v1/documents/uploads",
        headers={"Authorization": f"Bearer {token}"},
        files=[("files", ("policy.pdf", pdf_bytes(), "application/pdf"))],
    )

    assert response.status_code == 201
    item = response.json()["items"][0]
    assert item["storage_key"]
    assert item["processing_status"] == "queued"
    assert item["processing_error"] is None
    assert len(blob_storage_override.calls) == 1

    call = blob_storage_override.calls[0]
    assert call.content == pdf_bytes()
    assert call.content_type == "application/pdf"
    assert call.safe_filename == "policy.pdf"
    assert item["id"] == str(call.document_id)
    assert item["storage_key"] == build_document_blob_path(
        user_id=call.user_id,
        document_id=call.document_id,
        safe_filename=call.safe_filename,
        uploaded_at=call.uploaded_at,
    )

    async with AsyncSessionLocal() as session:
        document = await session.get(UploadedDocument, UUID(item["id"]))
    assert document is not None
    assert document.storage_key == item["storage_key"]
    assert processing_scheduler_override.scheduled_document_ids == [UUID(item["id"])]


@pytest.mark.anyio
async def test_admin_can_upload_multiple_document_files(
    client: AsyncClient,
    blob_storage_override: FakeBlobStorage,
    processing_scheduler_override: FakeProcessingScheduler,
) -> None:
    token = await admin_token(client)

    response = await client.post(
        "/api/v1/documents/uploads",
        headers={"Authorization": f"Bearer {token}"},
        files=[
            ("files", ("policy.pdf", pdf_bytes(), "application/pdf")),
            (
                "files",
                (
                    "program.docx",
                    docx_bytes(),
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                ),
            ),
            ("files", ("notes.txt", b"plain university notes", "text/plain")),
        ],
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["total"] == 3
    assert {item["original_filename"] for item in payload["items"]} == {
        "policy.pdf",
        "program.docx",
        "notes.txt",
    }
    assert all(item["uploaded_by_email"] == "admin@buro.com" for item in payload["items"])
    assert all(item["size_bytes"] > 0 for item in payload["items"])
    assert all(item["sha256_hash"] for item in payload["items"])
    assert all(item["storage_key"] for item in payload["items"])
    assert len(blob_storage_override.calls) == 3
    assert len({item["storage_key"] for item in payload["items"]}) == 3
    assert processing_scheduler_override.scheduled_document_ids == [
        UUID(item["id"]) for item in payload["items"]
    ]


@pytest.mark.anyio
async def test_default_user_cannot_upload_document_files(client: AsyncClient) -> None:
    auth_payload = await register_user(client, email="regular@example.com")

    response = await client.post(
        "/api/v1/documents/uploads",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
        files=[("files", ("policy.pdf", pdf_bytes(), "application/pdf"))],
    )

    assert response.status_code == 403


@pytest.mark.anyio
async def test_admin_can_initialize_direct_document_upload(client: AsyncClient) -> None:
    token = await admin_token(client)

    response = await client.post(
        "/api/v1/documents/uploads/direct/init",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "original_filename": "../../Course Plan.pdf",
            "content_type": "application/pdf",
            "size_bytes": len(pdf_bytes()),
            "sha256_hash": hashlib.sha256(pdf_bytes()).hexdigest(),
        },
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["status"] == "pending"
    assert payload["safe_filename"] == "Course_Plan.pdf"
    assert payload["content_type"] == "application/pdf"
    assert payload["size_bytes"] == len(pdf_bytes())
    assert payload["pathname"].endswith(f"{payload['document_id']}-Course_Plan.pdf")

    async with AsyncSessionLocal() as session:
        intent = await session.get(DocumentUploadIntent, UUID(payload["intent_id"]))
        document = await session.get(UploadedDocument, UUID(payload["document_id"]))

    assert intent is not None
    assert intent.status == DocumentUploadIntentStatus.PENDING
    assert intent.planned_pathname == payload["pathname"]
    assert document is None


@pytest.mark.anyio
async def test_direct_upload_init_rejects_invalid_metadata_and_non_admin(
    client: AsyncClient,
) -> None:
    token = await admin_token(client)
    auth_payload = await register_user(client, email="direct-regular@example.com")

    invalid_extension = await client.post(
        "/api/v1/documents/uploads/direct/init",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "original_filename": "malware.exe",
            "content_type": "application/octet-stream",
            "size_bytes": 123,
        },
    )
    wrong_content_type = await client.post(
        "/api/v1/documents/uploads/direct/init",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "original_filename": "policy.pdf",
            "content_type": "text/plain",
            "size_bytes": len(pdf_bytes()),
        },
    )
    too_large = await client.post(
        "/api/v1/documents/uploads/direct/init",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "original_filename": "large.txt",
            "content_type": "text/plain",
            "size_bytes": MAX_UPLOAD_FILE_SIZE_BYTES + 1,
        },
    )
    non_admin = await client.post(
        "/api/v1/documents/uploads/direct/init",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
        json={
            "original_filename": "policy.pdf",
            "content_type": "application/pdf",
            "size_bytes": len(pdf_bytes()),
        },
    )

    assert invalid_extension.status_code == 415
    assert wrong_content_type.status_code == 400
    assert too_large.status_code == 413
    assert non_admin.status_code == 403


@pytest.mark.anyio
async def test_admin_can_authorize_direct_document_upload(client: AsyncClient) -> None:
    token = await admin_token(client)
    init_response = await client.post(
        "/api/v1/documents/uploads/direct/init",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "original_filename": "policy.pdf",
            "content_type": "application/pdf",
            "size_bytes": len(pdf_bytes()),
        },
    )
    init_payload = init_response.json()

    response = await client.post(
        "/api/v1/documents/uploads/direct/authorize",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "intent_id": init_payload["intent_id"],
            "pathname": init_payload["pathname"],
            "content_type": "application/pdf",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["pathname"] == init_payload["pathname"]
    assert payload["allowed_content_types"] == ["application/pdf"]
    assert payload["add_random_suffix"] is False
    assert payload["allow_overwrite"] is False
    assert payload["token_payload"]["intentId"] == init_payload["intent_id"]


@pytest.mark.anyio
async def test_direct_upload_authorize_rejects_wrong_path_and_expired_intent(
    client: AsyncClient,
) -> None:
    token = await admin_token(client)
    init_response = await client.post(
        "/api/v1/documents/uploads/direct/init",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "original_filename": "policy.pdf",
            "content_type": "application/pdf",
            "size_bytes": len(pdf_bytes()),
        },
    )
    init_payload = init_response.json()

    wrong_path = await client.post(
        "/api/v1/documents/uploads/direct/authorize",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "intent_id": init_payload["intent_id"],
            "pathname": "documents/not-the-planned-path.pdf",
            "content_type": "application/pdf",
        },
    )

    async with AsyncSessionLocal() as session:
        intent = await session.get(DocumentUploadIntent, UUID(init_payload["intent_id"]))
        assert intent is not None
        intent.expires_at = utcnow() - timedelta(minutes=1)
        await session.commit()

    expired = await client.post(
        "/api/v1/documents/uploads/direct/authorize",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "intent_id": init_payload["intent_id"],
            "pathname": init_payload["pathname"],
            "content_type": "application/pdf",
        },
    )

    assert wrong_path.status_code == 400
    assert expired.status_code == 409


@pytest.mark.anyio
async def test_admin_can_complete_direct_document_upload(
    client: AsyncClient,
    blob_storage_override: FakeBlobStorage,
    processing_scheduler_override: FakeProcessingScheduler,
) -> None:
    token = await admin_token(client)
    content = pdf_bytes()
    init_response = await client.post(
        "/api/v1/documents/uploads/direct/init",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "original_filename": "policy.pdf",
            "content_type": "application/pdf",
            "size_bytes": len(content),
            "sha256_hash": hashlib.sha256(content).hexdigest(),
        },
    )
    init_payload = init_response.json()
    blob_storage_override.objects[init_payload["pathname"]] = content

    response = await client.post(
        "/api/v1/documents/uploads/direct/complete",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "intent_id": init_payload["intent_id"],
            "pathname": init_payload["pathname"],
            "url": f"https://blob.test/{init_payload['pathname']}",
            "download_url": f"https://blob.test/{init_payload['pathname']}?download=1",
            "etag": "etag-direct",
        },
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["id"] == init_payload["document_id"]
    assert payload["storage_key"] == init_payload["pathname"]
    assert payload["sha256_hash"] == hashlib.sha256(content).hexdigest()
    assert payload["processing_status"] == "queued"

    async with AsyncSessionLocal() as session:
        intent = await session.get(DocumentUploadIntent, UUID(init_payload["intent_id"]))
        document = await session.get(UploadedDocument, UUID(init_payload["document_id"]))

    assert intent is not None
    assert intent.status == DocumentUploadIntentStatus.COMPLETED
    assert intent.completed_at is not None
    assert intent.blob_etag == "etag-direct"
    assert document is not None
    assert document.storage_key == init_payload["pathname"]
    assert processing_scheduler_override.scheduled_document_ids == [UUID(payload["id"])]


@pytest.mark.anyio
async def test_direct_upload_complete_is_idempotent(
    client: AsyncClient,
    blob_storage_override: FakeBlobStorage,
    processing_scheduler_override: FakeProcessingScheduler,
) -> None:
    token = await admin_token(client)
    content = pdf_bytes()
    init_response = await client.post(
        "/api/v1/documents/uploads/direct/init",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "original_filename": "policy.pdf",
            "content_type": "application/pdf",
            "size_bytes": len(content),
        },
    )
    init_payload = init_response.json()
    blob_storage_override.objects[init_payload["pathname"]] = content
    completion_payload = {
        "intent_id": init_payload["intent_id"],
        "pathname": init_payload["pathname"],
        "url": f"https://blob.test/{init_payload['pathname']}",
        "download_url": f"https://blob.test/{init_payload['pathname']}?download=1",
    }

    first = await client.post(
        "/api/v1/documents/uploads/direct/complete",
        headers={"Authorization": f"Bearer {token}"},
        json=completion_payload,
    )
    second = await client.post(
        "/api/v1/documents/uploads/direct/complete",
        headers={"Authorization": f"Bearer {token}"},
        json=completion_payload,
    )

    assert first.status_code == 201
    assert second.status_code == 200
    assert first.json()["id"] == second.json()["id"]
    assert processing_scheduler_override.scheduled_document_ids == [UUID(first.json()["id"])]

    async with AsyncSessionLocal() as session:
        documents = (await session.execute(select(UploadedDocument))).scalars().all()

    assert len(documents) == 1


@pytest.mark.anyio
async def test_direct_upload_complete_rejects_invalid_blob_and_cleans_up(
    client: AsyncClient,
    blob_storage_override: FakeBlobStorage,
    processing_scheduler_override: FakeProcessingScheduler,
) -> None:
    token = await admin_token(client)
    init_response = await client.post(
        "/api/v1/documents/uploads/direct/init",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "original_filename": "policy.pdf",
            "content_type": "application/pdf",
            "size_bytes": len(b"not a pdf"),
            "sha256_hash": hashlib.sha256(b"different").hexdigest(),
        },
    )
    init_payload = init_response.json()
    blob_storage_override.objects[init_payload["pathname"]] = b"not a pdf"

    response = await client.post(
        "/api/v1/documents/uploads/direct/complete",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "intent_id": init_payload["intent_id"],
            "pathname": init_payload["pathname"],
            "url": f"https://blob.test/{init_payload['pathname']}",
            "download_url": f"https://blob.test/{init_payload['pathname']}?download=1",
        },
    )

    assert response.status_code == 400
    assert blob_storage_override.deleted_pathnames == [init_payload["pathname"]]
    assert processing_scheduler_override.scheduled_document_ids == []

    async with AsyncSessionLocal() as session:
        intent = await session.get(DocumentUploadIntent, UUID(init_payload["intent_id"]))
        document = await session.get(UploadedDocument, UUID(init_payload["document_id"]))

    assert intent is not None
    assert intent.status == DocumentUploadIntentStatus.FAILED
    assert intent.error_code == "hash_mismatch"
    assert document is None


@pytest.mark.anyio
async def test_admin_can_list_uploaded_documents_with_pagination(client: AsyncClient) -> None:
    token = await admin_token(client)
    await upload_pdf(client, filename="first-policy.pdf")
    await upload_pdf(client, filename="second-policy.pdf")

    response = await client.get(
        "/api/v1/documents",
        headers={"Authorization": f"Bearer {token}"},
        params={"limit": 1, "offset": 0},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 2
    assert payload["limit"] == 1
    assert payload["offset"] == 0
    assert len(payload["items"]) == 1
    assert payload["items"][0]["deleted_at"] is None
    assert payload["items"][0]["processing_status"] == "queued"


@pytest.mark.anyio
async def test_admin_can_search_documents_by_name_hash_and_uploader(client: AsyncClient) -> None:
    token = await admin_token(client)
    uploaded = await upload_pdf(client, filename="../../Course Plan.pdf")

    for search in ("Course Plan", "Course_Plan", uploaded["sha256_hash"][:12], "admin@buro.com"):
        response = await client.get(
            "/api/v1/documents",
            headers={"Authorization": f"Bearer {token}"},
            params={"search": search},
        )

        assert response.status_code == 200
        filenames = {item["safe_filename"] for item in response.json()["items"]}
        assert "Course_Plan.pdf" in filenames


@pytest.mark.anyio
async def test_default_user_cannot_list_or_search_documents(client: AsyncClient) -> None:
    auth_payload = await register_user(client, email="regular@example.com")

    response = await client.get(
        "/api/v1/documents",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
        params={"search": "policy"},
    )

    assert response.status_code == 403


@pytest.mark.anyio
async def test_admin_can_get_uploaded_document_by_id(client: AsyncClient) -> None:
    token = await admin_token(client)
    uploaded = await upload_pdf(client)

    response = await client.get(
        f"/api/v1/documents/{uploaded['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    assert response.json()["id"] == uploaded["id"]
    assert response.json()["processing_status"] == "queued"


@pytest.mark.anyio
async def test_default_user_cannot_get_uploaded_document_by_id(client: AsyncClient) -> None:
    uploaded = await upload_pdf(client)
    auth_payload = await register_user(client, email="regular@example.com")

    response = await client.get(
        f"/api/v1/documents/{uploaded['id']}",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
    )

    assert response.status_code == 403


@pytest.mark.anyio
async def test_admin_can_soft_delete_uploaded_document(
    client: AsyncClient,
    blob_storage_override: FakeBlobStorage,
) -> None:
    token = await admin_token(client)
    uploaded = await upload_pdf(client)

    delete_response = await client.delete(
        f"/api/v1/documents/{uploaded['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    get_response = await client.get(
        f"/api/v1/documents/{uploaded['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    list_response = await client.get(
        "/api/v1/documents",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert delete_response.status_code == 204
    assert get_response.status_code == 404
    assert uploaded["id"] not in {item["id"] for item in list_response.json()["items"]}
    assert blob_storage_override.deleted_pathnames == []


@pytest.mark.anyio
async def test_delete_missing_or_already_deleted_document_returns_404(client: AsyncClient) -> None:
    token = await admin_token(client)
    uploaded = await upload_pdf(client)
    await client.delete(
        f"/api/v1/documents/{uploaded['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )

    second_delete_response = await client.delete(
        f"/api/v1/documents/{uploaded['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    missing_delete_response = await client.delete(
        "/api/v1/documents/00000000-0000-0000-0000-000000000000",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert second_delete_response.status_code == 404
    assert missing_delete_response.status_code == 404


@pytest.mark.anyio
async def test_upload_rejects_unsupported_extension(
    client: AsyncClient,
    blob_storage_override: FakeBlobStorage,
) -> None:
    token = await admin_token(client)

    response = await client.post(
        "/api/v1/documents/uploads",
        headers={"Authorization": f"Bearer {token}"},
        files=[("files", ("script.exe", b"MZ executable", "application/octet-stream"))],
    )

    assert response.status_code == 415
    assert blob_storage_override.calls == []


@pytest.mark.anyio
async def test_blob_upload_failure_rolls_back_metadata_and_cleans_uploaded_blobs(
    client: AsyncClient,
    blob_storage_override: FakeBlobStorage,
) -> None:
    token = await admin_token(client)
    blob_storage_override.fail_on_call = 2

    response = await client.post(
        "/api/v1/documents/uploads",
        headers={"Authorization": f"Bearer {token}"},
        files=[
            ("files", ("policy.pdf", pdf_bytes(), "application/pdf")),
            ("files", ("notes.txt", b"plain university notes", "text/plain")),
        ],
    )

    assert response.status_code == 502
    assert len(blob_storage_override.calls) == 2
    assert len(blob_storage_override.deleted_pathnames) == 1

    first_call = blob_storage_override.calls[0]
    assert blob_storage_override.deleted_pathnames[0] == build_document_blob_path(
        user_id=first_call.user_id,
        document_id=first_call.document_id,
        safe_filename=first_call.safe_filename,
        uploaded_at=first_call.uploaded_at,
    )

    async with AsyncSessionLocal() as session:
        documents = (await session.execute(select(UploadedDocument))).scalars().all()
    assert documents == []


@pytest.mark.anyio
async def test_admin_can_queue_document_reprocessing(
    client: AsyncClient,
    processing_scheduler_override: FakeProcessingScheduler,
) -> None:
    token = await admin_token(client)
    uploaded = await upload_pdf(client)
    processing_scheduler_override.scheduled_document_ids.clear()

    response = await client.post(
        f"/api/v1/documents/{uploaded['id']}/processing",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 202
    assert response.json()["processing_status"] == "queued"
    assert processing_scheduler_override.scheduled_document_ids == [UUID(uploaded["id"])]


@pytest.mark.anyio
async def test_admin_can_get_latest_document_processing_metrics(client: AsyncClient) -> None:
    token = await admin_token(client)
    uploaded = await upload_pdf(client)

    no_run_response = await client.get(
        f"/api/v1/documents/{uploaded['id']}/processing",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert no_run_response.status_code == 200
    no_run_payload = no_run_response.json()
    assert no_run_payload["document_id"] == uploaded["id"]
    assert no_run_payload["processing_status"] == "queued"
    assert no_run_payload["latest_run"] is None

    now = utcnow()
    async with AsyncSessionLocal() as session:
        document = await session.get(UploadedDocument, UUID(uploaded["id"]))
        assert document is not None
        document.processing_status = DocumentProcessingStatus.COMPLETED
        document.processing_error = None
        document.processing_error_code = None
        document.processing_error_stage = None
        processing_run = DocumentProcessingRun(
            document_id=document.id,
            status=DocumentProcessingStatus.COMPLETED,
            started_at=now,
            completed_at=now,
            total_duration_ms=1234,
            stage_metrics_json={
                "parse": {
                    "status": "completed",
                    "started_at": now.isoformat(),
                    "completed_at": now.isoformat(),
                    "duration_ms": 300,
                    "page_count": 5,
                }
            },
            summary_metrics_json={
                "file_extension": "pdf",
                "size_bytes": uploaded["size_bytes"],
                "chunk_count": 7,
                "embedding_count": 7,
                "extraction_count": 2,
            },
        )
        session.add(processing_run)
        await session.commit()
        processing_run_id = processing_run.id

    response = await client.get(
        f"/api/v1/documents/{uploaded['id']}/processing",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["document_id"] == uploaded["id"]
    assert payload["processing_status"] == "completed"
    assert payload["processing_error"] is None
    assert payload["latest_run"]["id"] == str(processing_run_id)
    assert payload["latest_run"]["status"] == "completed"
    assert payload["latest_run"]["total_duration_ms"] == 1234
    assert payload["latest_run"]["stage_metrics"]["parse"]["duration_ms"] == 300
    assert payload["latest_run"]["summary_metrics"]["chunk_count"] == 7


@pytest.mark.anyio
async def test_default_user_cannot_get_document_processing_metrics(client: AsyncClient) -> None:
    uploaded = await upload_pdf(client)
    auth_payload = await register_user(client, email="regular-processing@example.com")

    response = await client.get(
        f"/api/v1/documents/{uploaded['id']}/processing",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
    )

    assert response.status_code == 403


@pytest.mark.anyio
async def test_admin_can_list_document_extraction_items(client: AsyncClient) -> None:
    token = await admin_token(client)
    uploaded = await upload_pdf(client)
    document_id = UUID(uploaded["id"])

    async with AsyncSessionLocal() as session:
        session.add_all(
            [
                DocumentExtractionItem(
                    document_id=document_id,
                    type="person",
                    value_json={"person_name": "Оскар Саєнко", "title": "Відповідальна особа"},
                    confidence=0.9,
                    source="openai",
                    evidence_text="Оскар Саєнко відповідає за подання документів.",
                    page_start=1,
                    page_end=1,
                    char_start=10,
                    char_end=58,
                ),
                DocumentExtractionItem(
                    document_id=document_id,
                    type="department",
                    value_json={"department": "Деканат ІКТА"},
                    confidence=0.85,
                    source="openai",
                    evidence_text="Документи подаються у деканат ІКТА.",
                    page_start=1,
                    page_end=1,
                    char_start=60,
                    char_end=98,
                ),
                DocumentExtractionItem(
                    document_id=document_id,
                    type="deadline",
                    value_json={"date": "2026-06-01"},
                    confidence=0.8,
                    source="openai",
                    evidence_text="Legacy deadline row should be hidden.",
                ),
            ]
        )
        await session.commit()

    response = await client.get(
        "/api/v1/documents/extraction-items?type=person",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 1
    assert payload["items"][0]["type"] == "person"
    assert payload["items"][0]["document_id"] == uploaded["id"]
    assert payload["items"][0]["document_filename"] == uploaded["safe_filename"]
    assert payload["items"][0]["value_json"]["person_name"] == "Оскар Саєнко"
    assert payload["items"][0]["evidence_text"] == "Оскар Саєнко відповідає за подання документів."

    all_response = await client.get(
        "/api/v1/documents/extraction-items",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert all_response.status_code == 200
    all_payload = all_response.json()
    assert all_payload["total"] == 1
    assert {item["type"] for item in all_payload["items"]} == {"person"}

    unsupported_type_response = await client.get(
        "/api/v1/documents/extraction-items?type=department",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert unsupported_type_response.status_code == 422

    search_response = await client.get(
        "/api/v1/documents/extraction-items?search=%D0%9E%D1%81%D0%BA%D0%B0%D1%80",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert search_response.status_code == 200
    search_payload = search_response.json()
    assert search_payload["total"] == 1
    assert search_payload["items"][0]["type"] == "person"


@pytest.mark.anyio
async def test_default_user_cannot_list_document_extraction_items(client: AsyncClient) -> None:
    auth_payload = await register_user(client, email="regular-extractions@example.com")

    response = await client.get(
        "/api/v1/documents/extraction-items",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
    )

    assert response.status_code == 403


@pytest.mark.anyio
async def test_reprocessing_document_without_blob_key_returns_conflict(
    client: AsyncClient,
    processing_scheduler_override: FakeProcessingScheduler,
) -> None:
    token = await admin_token(client)
    uploaded = await upload_pdf(client)
    processing_scheduler_override.scheduled_document_ids.clear()
    async with AsyncSessionLocal() as session:
        document = await session.get(UploadedDocument, UUID(uploaded["id"]))
        assert document is not None
        document.storage_key = None
        await session.commit()

    response = await client.post(
        f"/api/v1/documents/{uploaded['id']}/processing",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 409
    assert "Re-upload" in response.json()["detail"]
    assert processing_scheduler_override.scheduled_document_ids == []


@pytest.mark.anyio
async def test_upload_rejects_content_that_does_not_match_extension(client: AsyncClient) -> None:
    token = await admin_token(client)

    response = await client.post(
        "/api/v1/documents/uploads",
        headers={"Authorization": f"Bearer {token}"},
        files=[("files", ("fake.pdf", b"not a pdf", "application/pdf"))],
    )

    assert response.status_code == 400


@pytest.mark.anyio
async def test_upload_rejects_pdf_with_active_content(client: AsyncClient) -> None:
    token = await admin_token(client)

    response = await client.post(
        "/api/v1/documents/uploads",
        headers={"Authorization": f"Bearer {token}"},
        files=[
            (
                "files",
                ("active.pdf", b"%PDF-1.4\n/JavaScript /OpenAction\n%%EOF", "application/pdf"),
            )
        ],
    )

    assert response.status_code == 400


@pytest.mark.anyio
async def test_upload_allows_pdf_font_names_that_contain_aa(client: AsyncClient) -> None:
    token = await admin_token(client)

    response = await client.post(
        "/api/v1/documents/uploads",
        headers={"Authorization": f"Bearer {token}"},
        files=[
            (
                "files",
                ("font-name.pdf", pdf_with_font_name_containing_aa_bytes(), "application/pdf"),
            )
        ],
    )

    assert response.status_code == 201


@pytest.mark.anyio
async def test_upload_rejects_file_over_25mb(client: AsyncClient) -> None:
    token = await admin_token(client)

    response = await client.post(
        "/api/v1/documents/uploads",
        headers={"Authorization": f"Bearer {token}"},
        files=[
            (
                "files",
                ("large.txt", b"a" * (MAX_UPLOAD_FILE_SIZE_BYTES + 1), "text/plain"),
            )
        ],
    )

    assert response.status_code == 413


@pytest.mark.anyio
async def test_upload_sanitizes_unsafe_filename(client: AsyncClient) -> None:
    token = await admin_token(client)

    response = await client.post(
        "/api/v1/documents/uploads",
        headers={"Authorization": f"Bearer {token}"},
        files=[("files", ("../../course plan.pdf", pdf_bytes(), "application/pdf"))],
    )

    assert response.status_code == 201
    item = response.json()["items"][0]
    assert item["original_filename"] == "../../course plan.pdf"
    assert item["safe_filename"] == "course_plan.pdf"


def test_openapi_marks_upload_files_as_binary() -> None:
    app.openapi_schema = None
    schema = app.openapi()
    request_schema_ref = schema["paths"]["/api/v1/documents/uploads"]["post"]["requestBody"][
        "content"
    ]["multipart/form-data"]["schema"]["$ref"]
    schema_name = request_schema_ref.split("/")[-1]
    files_items_schema = schema["components"]["schemas"][schema_name]["properties"]["files"]["items"]

    assert files_items_schema == {"type": "string", "format": "binary"}
