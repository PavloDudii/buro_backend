from typing import Annotated

import pytest
from fastapi import Depends
from httpx import AsyncClient
from sqlalchemy import update

from src.core.dependencies import get_current_admin_user
from src.core.db.session import AsyncSessionLocal
from src.main import app
from src.models.user import User, UserRole
from src.tests.test_auth import register_user


@app.get("/admin-only")
async def admin_only(current_user: Annotated[User, Depends(get_current_admin_user)]) -> dict:
    return {"role": current_user.role}


@pytest.mark.anyio
async def test_patch_me_updates_full_name(client: AsyncClient) -> None:
    auth_payload = await register_user(client)

    response = await client.patch(
        "/api/v1/users/me",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
        json={"full_name": "Updated Name"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["email"] == auth_payload["user"]["email"]
    assert payload["full_name"] == "Updated Name"
    assert payload["role"] == "user"


@pytest.mark.anyio
async def test_patch_me_rejects_email_changes(client: AsyncClient) -> None:
    first_user = await register_user(client, email="first@example.com")

    response = await client.patch(
        "/api/v1/users/me",
        headers={"Authorization": f"Bearer {first_user['access_token']}"},
        json={"email": "updated@example.com"},
    )

    assert response.status_code == 422


@pytest.mark.anyio
async def test_admin_dependency_rejects_default_user(client: AsyncClient) -> None:
    auth_payload = await register_user(client)

    response = await client.get(
        "/admin-only",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
    )

    assert response.status_code == 403


@pytest.mark.anyio
async def test_admin_dependency_allows_admin(client: AsyncClient) -> None:
    auth_payload = await register_user(client, email="admin@example.com")
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(User).where(User.email == "admin@example.com").values(role=UserRole.ADMIN)
        )
        await session.commit()

    response = await client.get(
        "/admin-only",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
    )

    assert response.status_code == 200
    assert response.json() == {"role": "admin"}


@pytest.mark.anyio
async def test_bootstrap_admin_can_login(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/auth/login",
        json={"email": "admin@buro.com", "password": "AdminPassword123!"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["user"]["email"] == "admin@buro.com"
    assert payload["user"]["role"] == "admin"


@pytest.mark.anyio
async def test_admin_can_list_users_with_pagination(client: AsyncClient) -> None:
    admin_login = await client.post(
        "/api/v1/auth/login",
        json={"email": "admin@buro.com", "password": "AdminPassword123!"},
    )
    await register_user(client, email="alpha@example.com", full_name="Alpha User")
    await register_user(client, email="bravo@example.com", full_name="Bravo User")

    response = await client.get(
        "/api/v1/users",
        headers={"Authorization": f"Bearer {admin_login.json()['access_token']}"},
        params={"limit": 2, "offset": 0},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["total"] == 3
    assert payload["limit"] == 2
    assert payload["offset"] == 0
    assert len(payload["items"]) == 2
    assert all("role" in item for item in payload["items"])


@pytest.mark.anyio
async def test_admin_can_search_users_by_email_name_and_id(client: AsyncClient) -> None:
    admin_login = await client.post(
        "/api/v1/auth/login",
        json={"email": "admin@buro.com", "password": "AdminPassword123!"},
    )
    target = await register_user(
        client,
        email="search-target@example.com",
        full_name="Needle Person",
    )
    await register_user(client, email="other@example.com", full_name="Other Person")

    for search in (
        "SEARCH-target",
        "Needle",
        target["user"]["id"].split("-")[0],
    ):
        response = await client.get(
            "/api/v1/users",
            headers={"Authorization": f"Bearer {admin_login.json()['access_token']}"},
            params={"search": search},
        )

        assert response.status_code == 200
        payload = response.json()
        emails = {item["email"] for item in payload["items"]}
        assert "search-target@example.com" in emails


@pytest.mark.anyio
async def test_default_user_cannot_list_or_search_users(client: AsyncClient) -> None:
    user_payload = await register_user(client, email="regular@example.com")

    response = await client.get(
        "/api/v1/users",
        headers={"Authorization": f"Bearer {user_payload['access_token']}"},
        params={"search": "admin"},
    )

    assert response.status_code == 403


@pytest.mark.anyio
async def test_admin_can_get_user_by_id(client: AsyncClient) -> None:
    admin_login = await client.post(
        "/api/v1/auth/login",
        json={"email": "admin@buro.com", "password": "AdminPassword123!"},
    )
    target = await register_user(client, email="target@example.com")

    response = await client.get(
        f"/api/v1/users/{target['user']['id']}",
        headers={"Authorization": f"Bearer {admin_login.json()['access_token']}"},
    )

    assert response.status_code == 200
    assert response.json()["email"] == "target@example.com"


@pytest.mark.anyio
async def test_user_can_get_self_by_id(client: AsyncClient) -> None:
    auth_payload = await register_user(client, email="self@example.com")

    response = await client.get(
        f"/api/v1/users/{auth_payload['user']['id']}",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
    )

    assert response.status_code == 200
    assert response.json()["email"] == "self@example.com"


@pytest.mark.anyio
async def test_user_cannot_get_another_user_by_id(client: AsyncClient) -> None:
    auth_payload = await register_user(client, email="regular@example.com")
    target = await register_user(client, email="target@example.com")

    response = await client.get(
        f"/api/v1/users/{target['user']['id']}",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
    )

    assert response.status_code == 403


@pytest.mark.anyio
async def test_get_missing_user_by_id_returns_404(client: AsyncClient) -> None:
    admin_login = await client.post(
        "/api/v1/auth/login",
        json={"email": "admin@buro.com", "password": "AdminPassword123!"},
    )

    response = await client.get(
        "/api/v1/users/00000000-0000-0000-0000-000000000000",
        headers={"Authorization": f"Bearer {admin_login.json()['access_token']}"},
    )

    assert response.status_code == 404


@pytest.mark.anyio
async def test_admin_can_assign_and_revoke_roles(client: AsyncClient) -> None:
    admin_login = await client.post(
        "/api/v1/auth/login",
        json={"email": "admin@buro.com", "password": "AdminPassword123!"},
    )
    await register_user(client, email="target@example.com")

    promote_response = await client.patch(
        "/api/v1/users/role",
        headers={"Authorization": f"Bearer {admin_login.json()['access_token']}"},
        json={"email": "TARGET@Example.com", "role": "admin"},
    )

    assert promote_response.status_code == 200
    assert promote_response.json()["role"] == "admin"

    demote_response = await client.patch(
        "/api/v1/users/role",
        headers={"Authorization": f"Bearer {admin_login.json()['access_token']}"},
        json={"email": "target@example.com", "role": "user"},
    )

    assert demote_response.status_code == 200
    assert demote_response.json()["role"] == "user"


@pytest.mark.anyio
async def test_default_user_cannot_assign_roles(client: AsyncClient) -> None:
    user_payload = await register_user(client, email="regular@example.com")
    await register_user(client, email="target@example.com")

    response = await client.patch(
        "/api/v1/users/role",
        headers={"Authorization": f"Bearer {user_payload['access_token']}"},
        json={"email": "target@example.com", "role": "admin"},
    )

    assert response.status_code == 403


@pytest.mark.anyio
async def test_bootstrap_admin_cannot_be_demoted(client: AsyncClient) -> None:
    admin_login = await client.post(
        "/api/v1/auth/login",
        json={"email": "admin@buro.com", "password": "AdminPassword123!"},
    )

    response = await client.patch(
        "/api/v1/users/role",
        headers={"Authorization": f"Bearer {admin_login.json()['access_token']}"},
        json={"email": "admin@buro.com", "role": "user"},
    )

    assert response.status_code == 400
