import pytest
from httpx import AsyncClient

from src.services.institution import NULP_DEPARTMENTS
from src.tests.test_auth import register_user


async def admin_token(client: AsyncClient) -> str:
    response = await client.post(
        "/api/v1/auth/login",
        json={"email": "admin@buro.com", "password": "AdminPassword123!"},
    )
    assert response.status_code == 200
    return response.json()["access_token"]


@pytest.mark.anyio
async def test_admin_can_list_seeded_institutions(client: AsyncClient) -> None:
    token = await admin_token(client)

    response = await client.get(
        "/api/v1/institutions",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 18
    assert [item["code"] for item in payload["items"]] == [
        "ІАДУ",
        "ІАРД",
        "ІБІБ",
        "ІГДГ",
        "ІГСН",
        "ІНЕМ",
        "ІЕСК",
        "ІКТЕ",
        "ІКНІ",
        "ІКТА",
        "ІМІТ",
        "ІПМТ",
        "ІППТ",
        "ІППО",
        "ІМФН",
        "ІСТР",
        "ІХХТ",
        "МІОК",
    ]
    assert payload["items"][9]["name"] == "Комп'ютерні технології, автоматика та метрологія"
    assert all(item["is_active"] for item in payload["items"])


@pytest.mark.anyio
async def test_admin_can_list_seeded_departments_linked_to_institutions(client: AsyncClient) -> None:
    token = await admin_token(client)

    response = await client.get(
        "/api/v1/institutions/departments",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == len(NULP_DEPARTMENTS)
    assert payload["items"][0]["institution_code"] == "ІАДУ"
    assert (
        payload["items"][0]["institution_name"]
        == "Адміністрування, державного управління та професійного розвитку"
    )
    assert payload["items"][0]["name"] == "Кафедра адміністративного та фінансового менеджменту"
    assert all(item["institution_id"] for item in payload["items"])

    ikta_response = await client.get(
        "/api/v1/institutions/departments?institution_code=%D0%86%D0%9A%D0%A2%D0%90",
        headers={"Authorization": f"Bearer {token}"},
    )

    assert ikta_response.status_code == 200
    ikta_payload = ikta_response.json()
    assert ikta_payload["total"] == 7
    assert [item["name"] for item in ikta_payload["items"]] == [
        "Кафедра безпеки інформаційних технологій",
        "Кафедра електронних обчислювальних машин",
        "Кафедра захисту інформації",
        "Кафедра інтелектуальної мехатроніки і роботики",
        "Кафедра інформаційно-вимірювальних технологій",
        "Кафедра комп'ютеризованих систем автоматики",
        "Кафедра спеціалізованих комп'ютерних систем",
    ]


@pytest.mark.anyio
async def test_default_user_cannot_list_institutions(client: AsyncClient) -> None:
    auth_payload = await register_user(client, email="regular-institutions@example.com")

    response = await client.get(
        "/api/v1/institutions",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
    )

    assert response.status_code == 403

    departments_response = await client.get(
        "/api/v1/institutions/departments",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
    )

    assert departments_response.status_code == 403
