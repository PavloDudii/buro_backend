import pytest
from httpx import AsyncClient


async def register_user(
    client: AsyncClient,
    *,
    email: str = "user@example.com",
    full_name: str = "Test User",
    password: str = "Password123",
) -> dict:
    response = await client.post(
        "/api/v1/auth/register",
        json={"email": email, "full_name": full_name, "password": password},
    )
    assert response.status_code == 201, response.text
    return response.json()


@pytest.mark.anyio
async def test_register_user_success(client: AsyncClient) -> None:
    payload = await register_user(client, email="USER@Example.com")

    assert payload["user"]["email"] == "user@example.com"
    assert payload["user"]["full_name"] == "Test User"
    assert payload["user"]["role"] == "user"
    assert payload["access_token"]
    assert payload["refresh_token"]
    assert payload["token_type"] == "bearer"


@pytest.mark.anyio
async def test_register_duplicate_email_fails(client: AsyncClient) -> None:
    await register_user(client)

    response = await client.post(
        "/api/v1/auth/register",
        json={"email": "user@example.com", "full_name": "Other User", "password": "Password123"},
    )

    assert response.status_code == 409


@pytest.mark.anyio
async def test_login_success_with_normalized_email(client: AsyncClient) -> None:
    await register_user(client, email="mixed@example.com", password="Password123")

    response = await client.post(
        "/api/v1/auth/login",
        json={"email": "MIXED@EXAMPLE.COM", "password": "Password123"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["user"]["email"] == "mixed@example.com"
    assert payload["user"]["role"] == "user"
    assert payload["access_token"]
    assert payload["refresh_token"]


@pytest.mark.anyio
async def test_login_fails_with_wrong_password(client: AsyncClient) -> None:
    await register_user(client)

    response = await client.post(
        "/api/v1/auth/login",
        json={"email": "user@example.com", "password": "WrongPass123"},
    )

    assert response.status_code == 401


@pytest.mark.anyio
async def test_access_token_protects_me_endpoint(client: AsyncClient) -> None:
    response = await client.get("/api/v1/users/me")
    assert response.status_code == 401

    auth_payload = await register_user(client)
    response = await client.get(
        "/api/v1/users/me",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
    )

    assert response.status_code == 200
    assert response.json()["email"] == "user@example.com"


@pytest.mark.anyio
async def test_refresh_rotates_tokens_and_invalidates_old_refresh_token(client: AsyncClient) -> None:
    auth_payload = await register_user(client)

    refresh_response = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": auth_payload["refresh_token"]},
    )

    assert refresh_response.status_code == 200
    rotated = refresh_response.json()
    assert rotated["refresh_token"] != auth_payload["refresh_token"]

    reuse_response = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": auth_payload["refresh_token"]},
    )

    assert reuse_response.status_code == 401


@pytest.mark.anyio
async def test_logout_revokes_refresh_token(client: AsyncClient) -> None:
    auth_payload = await register_user(client)

    logout_response = await client.post(
        "/api/v1/auth/logout",
        json={"refresh_token": auth_payload["refresh_token"]},
    )
    assert logout_response.status_code == 204

    refresh_response = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": auth_payload["refresh_token"]},
    )
    assert refresh_response.status_code == 401


@pytest.mark.anyio
async def test_change_password_rejects_wrong_current_password(client: AsyncClient) -> None:
    auth_payload = await register_user(client)

    response = await client.post(
        "/api/v1/auth/change-password",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
        json={"current_password": "WrongPass123", "new_password": "NewPassword123"},
    )

    assert response.status_code == 400


@pytest.mark.anyio
async def test_change_password_revokes_prior_refresh_sessions(client: AsyncClient) -> None:
    auth_payload = await register_user(client)

    response = await client.post(
        "/api/v1/auth/change-password",
        headers={"Authorization": f"Bearer {auth_payload['access_token']}"},
        json={"current_password": "Password123", "new_password": "NewPassword123"},
    )

    assert response.status_code == 200

    refresh_response = await client.post(
        "/api/v1/auth/refresh",
        json={"refresh_token": auth_payload["refresh_token"]},
    )
    assert refresh_response.status_code == 401

    old_login = await client.post(
        "/api/v1/auth/login",
        json={"email": "user@example.com", "password": "Password123"},
    )
    assert old_login.status_code == 401

    new_login = await client.post(
        "/api/v1/auth/login",
        json={"email": "user@example.com", "password": "NewPassword123"},
    )
    assert new_login.status_code == 200
