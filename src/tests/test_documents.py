from io import BytesIO
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
from httpx import AsyncClient

from src.main import app
from src.tests.test_auth import register_user


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


@pytest.mark.anyio
async def test_admin_can_upload_multiple_document_files(client: AsyncClient) -> None:
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
async def test_admin_can_soft_delete_uploaded_document(client: AsyncClient) -> None:
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
async def test_upload_rejects_unsupported_extension(client: AsyncClient) -> None:
    token = await admin_token(client)

    response = await client.post(
        "/api/v1/documents/uploads",
        headers={"Authorization": f"Bearer {token}"},
        files=[("files", ("script.exe", b"MZ executable", "application/octet-stream"))],
    )

    assert response.status_code == 415


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
async def test_upload_rejects_file_over_10mb(client: AsyncClient) -> None:
    token = await admin_token(client)

    response = await client.post(
        "/api/v1/documents/uploads",
        headers={"Authorization": f"Bearer {token}"},
        files=[("files", ("large.txt", b"a" * (10 * 1024 * 1024 + 1), "text/plain"))],
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
