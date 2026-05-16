from collections.abc import Iterator, Sequence
from uuid import UUID

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select

from src.core.db.session import AsyncSessionLocal
from src.core.dependencies import (
    get_blob_storage,
    get_document_processing_scheduler,
    get_program_import_client,
)
from src.main import app
from src.models.document import UploadedDocument
from src.models.institution import Department
from src.models.program import (
    EducationProgram,
    ProgramDirectorySnapshot,
    ProgramDocument,
    ProgramDocumentImportStatus,
)
from src.services.blob_storage import StoredBlob, build_document_blob_path
from src.services.program_import import (
    BACHELOR_PROGRAM_SOURCE_URL,
    DownloadedRemoteFile,
    RemoteFileTooLargeError,
    match_department_for_program,
    parse_bachelor_programs_html,
    parse_directory_program_metadata,
)
from src.tests.test_document_ingestion import pdf_with_text


DIRECTORY_URL = "https://directory.lpnu.ua/majors/ikni/126/8/2025/ua/full"
OPP_URL = "https://lpnu.ua/sites/default/files/2025/opp.pdf"
OVERSIZED_URL = "https://lpnu.ua/sites/default/files/2025/self-evaluation.pdf"
SECOND_DIRECTORY_URL = "https://directory.lpnu.ua/majors/ikni/122/8/2025/ua/full"
SECOND_OPP_URL = "https://lpnu.ua/sites/default/files/2025/second-opp.pdf"


def bachelor_html() -> str:
    return f"""
    <html>
      <body>
        <table>
          <tr><td colspan="8">Галузь знань: 12 Інформаційні технології</td></tr>
          <tr>
            <td>126 «Інформаційні системи та технології»</td>
            <td>«Інформаційні системи та технології»</td>
            <td><a href="{OPP_URL}">ОПП 2025</a> 8,9 МБ</td>
            <td><a href="{DIRECTORY_URL}">2025</a></td>
            <td><a href="{OVERSIZED_URL}">Відомості про самооцінювання ОП</a> 55 МБ</td>
            <td><a href="/files/visit.pdf">Програма та розклад роботи експертної групи</a></td>
            <td><a href="/files/report.pdf">Звіт експертної групи</a></td>
            <td><a href="/files/certificate.pdf">Сертифікат</a></td>
          </tr>
        </table>
      </body>
    </html>
    """


def current_bachelor_html() -> str:
    return """
    <html>
      <body>
        <table>
          <caption>Галузь знань: 12 Інформаційні технології</caption>
          <thead>
            <tr>
              <th>Назва спеціальності</th>
              <th>Найменування освітньої програми</th>
              <th>Освітньо-професійна програма</th>
              <th>Силабуси освітніх компонентів</th>
            </tr>
          </thead>
          <tbody>
            <tr class="program-row">
              <td>126 «Інформаційні системи та технології»</td>
              <td>«Інформаційні системи та технології»</td>
              <td>
                <span class="file">
                  <span class="file-link">
                    <a href="https://lpnu.ua/sites/default/files/2025/opp.PDF">ОПП-2025</a>
                  </span>
                  <span class="file-size">8.95 МБ</span>
                </span>
              </td>
              <td>
                <a href="https://directory.lpnu.ua/majors/ikni/6.126.00.05/8/2024/ua/full">2024</a>,
                <a href="https://directory-new.lpnu.ua/majors/ikni/6.126.00.05/8/2025/ua/full">2025</a>
              </td>
            </tr>
          </tbody>
        </table>
      </body>
    </html>
    """


def two_program_bachelor_html() -> str:
    return f"""
    <html>
      <body>
        <table>
          <tr><td colspan="8">Галузь знань: 12 Інформаційні технології</td></tr>
          <tr>
            <td>126 «Інформаційні системи та технології»</td>
            <td>«Інформаційні системи та технології»</td>
            <td><a href="{OPP_URL}">ОПП 2025</a> 8,9 МБ</td>
            <td><a href="{DIRECTORY_URL}">2025</a></td>
            <td><a href="{OVERSIZED_URL}">Відомості про самооцінювання ОП</a> 55 МБ</td>
            <td><a href="/files/visit.pdf">Програма та розклад роботи експертної групи</a></td>
            <td><a href="/files/report.pdf">Звіт експертної групи</a></td>
            <td><a href="/files/certificate.pdf">Сертифікат</a></td>
          </tr>
          <tr>
            <td>122 «Комп'ютерні науки»</td>
            <td>«Комп'ютерні науки»</td>
            <td><a href="{SECOND_OPP_URL}">ОПП 2025</a> 4 МБ</td>
            <td><a href="{SECOND_DIRECTORY_URL}">2025</a></td>
          </tr>
        </table>
      </body>
    </html>
    """


def directory_html() -> str:
    return """
    <html>
      <body>
        <h1>Інформаційні системи та технології</h1>
        <dl>
          <dt>Кваліфікація</dt><dd>Бакалавр з інформаційних систем та технологій</dd>
          <dt>Інститут</dt><dd>ІКНІ - Комп'ютерні науки та інформаційні технології</dd>
          <dt>Форма навчання</dt><dd>денна</dd>
          <dt>Тривалість програми</dt><dd>3 роки 10 місяців</dd>
          <dt>Кількість кредитів</dt><dd>240 кредитів ЄКТС</dd>
          <dt>Керівник освітньої програми</dt><dd>Оксана Приклад</dd>
        </dl>
        <p>Характеристика освітньої програми: Програма орієнтована на практику.</p>
        <h2>Освітні компоненти</h2>
        <p>1 семестр</p>
        <p>Обов'язкові дисципліни</p>
        <p>Аналітична геометрія</p>
      </body>
    </html>
    """


def current_directory_html() -> str:
    return """
    <html>
      <body>
        <h1>Інформаційні системи та технології</h1>
        <p>Кваліфікація: Бакалавр з інформаційних систем та технологій</p>
        <p>Рік вступу: 2025</p>
        <p>Форма навчання: денна</p>
        <p>Тривалість програми: 4 роки</p>
        <p>Інститут: Інститут комп'ютерних наук та інформаційних технологій</p>
        <p>Кількість кредитів: 240 кредитів ЄКТС</p>
        <p>Керівник освітньої програми, контактна особа: Оксана Приклад</p>
        <p>Характеристика освітньої програми: Програма орієнтована на практику.</p>
        <p>Програмні результати навчання: 1. Знати стандарти. 2. Використовувати технології.</p>
        <h2>Освітні компоненти</h2>
        <p>1 семестр</p>
        <p>Обов'язкові дисципліни</p>
        <p>Аналітична геометрія</p>
      </body>
    </html>
    """


class FakeProgramImportClient:
    def __init__(self) -> None:
        self.texts = {
            BACHELOR_PROGRAM_SOURCE_URL: bachelor_html(),
            DIRECTORY_URL: directory_html(),
        }
        self.files = {
            OPP_URL: DownloadedRemoteFile(
                content=pdf_with_text()
                + b"\n"
                + "Кафедра інформаційних систем та мереж".encode(),
                content_type="application/pdf",
            ),
            "https://lpnu.ua/files/visit.pdf": DownloadedRemoteFile(
                content=pdf_with_text(),
                content_type="application/pdf",
            ),
            "https://lpnu.ua/files/report.pdf": DownloadedRemoteFile(
                content=pdf_with_text(),
                content_type="application/pdf",
            ),
            "https://lpnu.ua/files/certificate.pdf": DownloadedRemoteFile(
                content=pdf_with_text(),
                content_type="application/pdf",
            ),
            SECOND_OPP_URL: DownloadedRemoteFile(
                content=pdf_with_text(),
                content_type="application/pdf",
            ),
        }
        self.text_calls: list[str] = []
        self.file_calls: list[str] = []

    async def get_text(self, url: str) -> str:
        self.text_calls.append(url)
        return self.texts[url]

    async def get_file(self, url: str, *, max_bytes: int) -> DownloadedRemoteFile:
        self.file_calls.append(url)
        if url == OVERSIZED_URL:
            raise RemoteFileTooLargeError(size_bytes=55 * 1024 * 1024, max_bytes=max_bytes)
        return self.files[url]


class FakeBlobStorage:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.deleted_pathnames: list[str] = []

    async def put_document(
        self,
        *,
        user_id: UUID,
        document_id: UUID,
        safe_filename: str,
        content: bytes,
        content_type: str,
        uploaded_at,
    ) -> StoredBlob:
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
        )

    async def get_document_content(self, pathname: str) -> bytes:
        return self.objects[pathname]

    async def delete_documents(self, pathnames: Sequence[str]) -> None:
        self.deleted_pathnames.extend(pathnames)


class FakeProcessingScheduler:
    def __init__(self) -> None:
        self.scheduled_document_ids: list[UUID] = []

    def schedule_documents(self, document_ids: Sequence[UUID]) -> None:
        self.scheduled_document_ids.extend(document_ids)


@pytest.fixture
def program_import_overrides() -> Iterator[tuple[FakeProgramImportClient, FakeBlobStorage, FakeProcessingScheduler]]:
    client = FakeProgramImportClient()
    blob_storage = FakeBlobStorage()
    scheduler = FakeProcessingScheduler()
    app.dependency_overrides[get_program_import_client] = lambda: client
    app.dependency_overrides[get_blob_storage] = lambda: blob_storage
    app.dependency_overrides[get_document_processing_scheduler] = lambda: scheduler
    yield client, blob_storage, scheduler
    app.dependency_overrides.pop(get_program_import_client, None)
    app.dependency_overrides.pop(get_blob_storage, None)
    app.dependency_overrides.pop(get_document_processing_scheduler, None)


async def admin_token(client: AsyncClient) -> str:
    response = await client.post(
        "/api/v1/auth/login",
        json={"email": "admin@buro.com", "password": "AdminPassword123!"},
    )
    assert response.status_code == 200
    return response.json()["access_token"]


def test_bachelor_program_parser_extracts_programs_and_linked_documents() -> None:
    programs = parse_bachelor_programs_html(
        bachelor_html(),
        source_url=BACHELOR_PROGRAM_SOURCE_URL,
    )

    assert len(programs) == 1
    program = programs[0]
    assert program.field_code == "12"
    assert program.field_name == "Інформаційні технології"
    assert program.specialty_code == "126"
    assert program.specialty_name == "Інформаційні системи та технології"
    assert program.program_name == "Інформаційні системи та технології"
    assert program.program_url == DIRECTORY_URL
    assert [document.kind for document in program.documents] == [
        "opp",
        "self_evaluation",
        "visit_schedule",
        "accreditation_report",
        "certificate",
    ]
    assert program.documents[0].source_size_label == "8,9 МБ"
    assert program.documents[1].source_size_label == "55 МБ"


def test_bachelor_program_parser_extracts_current_caption_table_markup() -> None:
    programs = parse_bachelor_programs_html(
        current_bachelor_html(),
        source_url=BACHELOR_PROGRAM_SOURCE_URL,
    )

    assert len(programs) == 1
    program = programs[0]
    assert program.field_code == "12"
    assert program.field_name == "Інформаційні технології"
    assert program.specialty_code == "126"
    assert program.program_name == "Інформаційні системи та технології"
    assert program.program_url == "https://directory-new.lpnu.ua/majors/ikni/6.126.00.05/8/2025/ua/full"
    assert len(program.documents) == 1
    assert program.documents[0].source_url == "https://lpnu.ua/sites/default/files/2025/opp.PDF"
    assert program.documents[0].source_size_label == "8.95 МБ"


def test_directory_program_metadata_parser_extracts_structured_fields() -> None:
    metadata = parse_directory_program_metadata(directory_html(), program_url=DIRECTORY_URL)

    assert metadata.qualification == "Бакалавр з інформаційних систем та технологій"
    assert metadata.institution_text == "ІКНІ - Комп'ютерні науки та інформаційні технології"
    assert metadata.study_form == "денна"
    assert metadata.duration == "3 роки 10 місяців"
    assert metadata.credits == "240 кредитів ЄКТС"
    assert metadata.manager == "Оксана Приклад"
    assert "Характеристика освітньої програми" in {section["title"] for section in metadata.sections}
    assert metadata.structured["освітні компоненти"][0]["semester"] == "1 семестр"
    assert "Аналітична геометрія" in metadata.raw_text


def test_directory_program_metadata_parser_extracts_current_inline_fields() -> None:
    metadata = parse_directory_program_metadata(
        current_directory_html(),
        program_url=DIRECTORY_URL,
    )

    assert metadata.qualification == "Бакалавр з інформаційних систем та технологій"
    assert metadata.institution_text == "Інститут комп'ютерних наук та інформаційних технологій"
    assert metadata.study_form == "денна"
    assert metadata.duration == "4 роки"
    assert metadata.credits == "240 кредитів ЄКТС"
    assert metadata.manager == "Оксана Приклад"


@pytest.mark.anyio
async def test_department_matching_requires_one_canonical_department() -> None:
    async with AsyncSessionLocal() as session:
        ikni_departments = (
            await session.execute(
                select(Department)
                .join(Department.institution)
                .where(Department.institution.has(code="ІКНІ"))
            )
        ).scalars().all()

    matched = match_department_for_program(
        "Текст ОПП. Кафедра інформаційних систем та мереж забезпечує програму.",
        departments=ikni_departments,
    )
    assert matched.department is not None
    assert matched.department.name == "Кафедра інформаційних систем та мереж"
    assert matched.status == "matched"

    pending = match_department_for_program(
        "Текст ОПП без кафедри.",
        departments=ikni_departments,
    )
    assert pending.department is None
    assert pending.status == "pending_review"


@pytest.mark.anyio
async def test_admin_imports_bachelor_programs_idempotently(
    client: AsyncClient,
    program_import_overrides: tuple[FakeProgramImportClient, FakeBlobStorage, FakeProcessingScheduler],
) -> None:
    token = await admin_token(client)
    fake_client, blob_storage, scheduler = program_import_overrides

    first = await client.post(
        "/api/v1/programs/import/nulp/bachelor",
        headers={"Authorization": f"Bearer {token}"},
    )
    second = await client.post(
        "/api/v1/programs/import/nulp/bachelor",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert first.status_code == 202
    assert second.status_code == 202
    first_payload = first.json()
    assert first_payload["program_count"] == 1
    assert first_payload["created_document_count"] == 4
    assert first_payload["oversized_document_count"] == 1
    assert first_payload["matched_program_count"] == 1
    assert second.json()["program_count"] == 0
    assert second.json()["created_document_count"] == 0
    assert len(blob_storage.objects) == 4
    assert len(scheduler.scheduled_document_ids) == 4
    assert OPP_URL in fake_client.file_calls
    assert OVERSIZED_URL not in fake_client.file_calls

    async with AsyncSessionLocal() as session:
        program_count = await session.scalar(select(func.count()).select_from(EducationProgram))
        document_count = await session.scalar(select(func.count()).select_from(UploadedDocument))
        program_document_count = await session.scalar(select(func.count()).select_from(ProgramDocument))
        snapshot_count = await session.scalar(select(func.count()).select_from(ProgramDirectorySnapshot))
        program = (await session.execute(select(EducationProgram))).scalar_one()
        program_documents = (
            await session.execute(
                select(ProgramDocument).order_by(ProgramDocument.source_url.asc())
            )
        ).scalars().all()

    assert program_count == 1
    assert document_count == 4
    assert program_document_count == 5
    assert snapshot_count == 1
    assert program.institution is not None
    assert program.institution.code == "ІКНІ"
    assert program.department is not None
    assert program.department.name == "Кафедра інформаційних систем та мереж"
    assert program.department_link_status == "matched"
    assert any(
        document.import_status == ProgramDocumentImportStatus.OVERSIZED
        and document.uploaded_document_id is None
        for document in program_documents
    )


@pytest.mark.anyio
async def test_admin_imports_one_bachelor_program_per_request(
    client: AsyncClient,
    program_import_overrides: tuple[FakeProgramImportClient, FakeBlobStorage, FakeProcessingScheduler],
) -> None:
    token = await admin_token(client)
    fake_client, blob_storage, scheduler = program_import_overrides
    fake_client.texts[BACHELOR_PROGRAM_SOURCE_URL] = two_program_bachelor_html()
    fake_client.texts[SECOND_DIRECTORY_URL] = directory_html()

    first = await client.post(
        "/api/v1/programs/import/nulp/bachelor",
        headers={"Authorization": f"Bearer {token}"},
    )
    second = await client.post(
        "/api/v1/programs/import/nulp/bachelor",
        headers={"Authorization": f"Bearer {token}"},
    )
    third = await client.post(
        "/api/v1/programs/import/nulp/bachelor",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert first.status_code == 202
    assert second.status_code == 202
    assert third.status_code == 202
    assert first.json()["program_count"] == 1
    assert first.json()["created_document_count"] == 4
    assert second.json()["program_count"] == 1
    assert second.json()["created_document_count"] == 1
    assert third.json()["program_count"] == 0
    assert third.json()["created_document_count"] == 0
    assert SECOND_OPP_URL in fake_client.file_calls
    assert len(blob_storage.objects) == 5
    assert len(scheduler.scheduled_document_ids) == 5

    async with AsyncSessionLocal() as session:
        program_count = await session.scalar(select(func.count()).select_from(EducationProgram))
        document_count = await session.scalar(select(func.count()).select_from(UploadedDocument))
        program_document_count = await session.scalar(select(func.count()).select_from(ProgramDocument))

    assert program_count == 2
    assert document_count == 5
    assert program_document_count == 6


@pytest.mark.anyio
async def test_admin_can_list_programs_and_details(
    client: AsyncClient,
    program_import_overrides: tuple[FakeProgramImportClient, FakeBlobStorage, FakeProcessingScheduler],
) -> None:
    token = await admin_token(client)
    await client.post(
        "/api/v1/programs/import/nulp/bachelor",
        headers={"Authorization": f"Bearer {token}"},
    )

    list_response = await client.get(
        "/api/v1/programs?department_link_status=matched&search=%D1%81%D0%B8%D1%81%D1%82%D0%B5%D0%BC",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert list_response.status_code == 200
    payload = list_response.json()
    assert payload["total"] == 1
    item = payload["items"][0]
    assert item["level"] == "bachelor"
    assert item["institution_code"] == "ІКНІ"
    assert item["department_name"] == "Кафедра інформаційних систем та мереж"
    assert item["document_count"] == 5
    assert item["downloaded_document_count"] == 4
    assert item["oversized_document_count"] == 1

    detail_response = await client.get(
        f"/api/v1/programs/{item['id']}",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert detail_response.status_code == 200
    detail = detail_response.json()
    assert detail["id"] == item["id"]
    assert len(detail["documents"]) == 5
    assert {document["kind"] for document in detail["documents"]} >= {
        "opp",
        "self_evaluation",
        "visit_schedule",
        "accreditation_report",
        "certificate",
    }
    assert any(document["processing_status"] == "queued" for document in detail["documents"])


@pytest.mark.anyio
async def test_admin_can_manually_assign_program_department(
    client: AsyncClient,
    program_import_overrides: tuple[FakeProgramImportClient, FakeBlobStorage, FakeProcessingScheduler],
) -> None:
    token = await admin_token(client)
    await client.post(
        "/api/v1/programs/import/nulp/bachelor",
        headers={"Authorization": f"Bearer {token}"},
    )
    async with AsyncSessionLocal() as session:
        program = (await session.execute(select(EducationProgram))).scalar_one()
        department = (
            await session.execute(
                select(Department)
                .join(Department.institution)
                .where(
                    Department.institution.has(code="ІКНІ"),
                    Department.name == "Кафедра програмного забезпечення",
                )
            )
        ).scalar_one()

    response = await client.patch(
        f"/api/v1/programs/{program.id}/department",
        headers={"Authorization": f"Bearer {token}"},
        json={"department_id": str(department.id)},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["department_name"] == "Кафедра програмного забезпечення"
    assert payload["department_link_status"] == "manual"


@pytest.mark.anyio
async def test_admin_can_edit_program_document_and_soft_delete_program(
    client: AsyncClient,
    program_import_overrides: tuple[FakeProgramImportClient, FakeBlobStorage, FakeProcessingScheduler],
) -> None:
    token = await admin_token(client)
    await client.post(
        "/api/v1/programs/import/nulp/bachelor",
        headers={"Authorization": f"Bearer {token}"},
    )
    async with AsyncSessionLocal() as session:
        program = (await session.execute(select(EducationProgram))).scalar_one()
        program_document = (
            await session.execute(select(ProgramDocument).where(ProgramDocument.program_id == program.id))
        ).scalars().first()

    edit_response = await client.patch(
        f"/api/v1/programs/{program.id}",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "program_name": "Оновлена програма",
            "qualification": "Оновлена кваліфікація",
        },
    )
    document_response = await client.patch(
        f"/api/v1/programs/{program.id}/documents/{program_document.id}",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "title": "Оновлений документ",
            "kind": "other",
            "import_error": "manual note",
        },
    )
    delete_response = await client.delete(
        f"/api/v1/programs/{program.id}",
        headers={"Authorization": f"Bearer {token}"},
    )
    list_response = await client.get(
        "/api/v1/programs",
        headers={"Authorization": f"Bearer {token}"},
    )
    detail_response = await client.get(
        f"/api/v1/programs/{program.id}",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert edit_response.status_code == 200
    assert edit_response.json()["program_name"] == "Оновлена програма"
    assert edit_response.json()["qualification"] == "Оновлена кваліфікація"
    assert document_response.status_code == 200
    assert document_response.json()["title"] == "Оновлений документ"
    assert document_response.json()["kind"] == "other"
    assert document_response.json()["import_error"] == "manual note"
    assert delete_response.status_code == 204
    assert list_response.json()["total"] == 0
    assert detail_response.status_code == 404

    async with AsyncSessionLocal() as session:
        deleted_program = await session.get(EducationProgram, program.id)
        document_count = await session.scalar(select(func.count()).select_from(UploadedDocument))

    assert deleted_program is not None
    assert deleted_program.deleted_at is not None
    assert document_count == 4


@pytest.mark.anyio
async def test_program_import_requires_admin(
    client: AsyncClient,
    program_import_overrides: tuple[FakeProgramImportClient, FakeBlobStorage, FakeProcessingScheduler],
) -> None:
    register_response = await client.post(
        "/api/v1/auth/register",
        json={
            "email": "regular-programs@example.com",
            "full_name": "Regular User",
            "password": "Password123!",
        },
    )

    response = await client.post(
        "/api/v1/programs/import/nulp/bachelor",
        headers={"Authorization": f"Bearer {register_response.json()['access_token']}"},
    )
    edit_response = await client.patch(
        f"/api/v1/programs/{UUID(int=0)}",
        headers={"Authorization": f"Bearer {register_response.json()['access_token']}"},
        json={"program_name": "Nope"},
    )
    delete_response = await client.delete(
        f"/api/v1/programs/{UUID(int=0)}",
        headers={"Authorization": f"Bearer {register_response.json()['access_token']}"},
    )

    assert response.status_code == 403
    assert edit_response.status_code == 403
    assert delete_response.status_code == 403


@pytest.mark.anyio
async def test_imported_documents_show_program_source_badge(
    client: AsyncClient,
    program_import_overrides: tuple[FakeProgramImportClient, FakeBlobStorage, FakeProcessingScheduler],
) -> None:
    token = await admin_token(client)
    await client.post(
        "/api/v1/programs/import/nulp/bachelor",
        headers={"Authorization": f"Bearer {token}"},
    )

    response = await client.get(
        "/api/v1/documents",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 4
    assert {item["source_type"] for item in payload["items"]} == {"program"}
    assert {item["program_name"] for item in payload["items"]} == {
        "Інформаційні системи та технології"
    }
