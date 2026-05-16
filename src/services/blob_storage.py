import inspect
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from src.core.settings import Settings


@dataclass(frozen=True)
class StoredBlob:
    pathname: str
    url: str
    download_url: str
    etag: str | None = None


class BlobStorage(Protocol):
    async def put_document(
        self,
        *,
        user_id: uuid.UUID,
        document_id: uuid.UUID,
        safe_filename: str,
        content: bytes,
        content_type: str,
        uploaded_at: datetime,
    ) -> StoredBlob:
        ...

    async def delete_documents(self, pathnames: Sequence[str]) -> None:
        ...

    async def get_document_content(self, pathname: str) -> bytes:
        ...


def build_document_blob_path(
    *,
    user_id: uuid.UUID,
    document_id: uuid.UUID,
    safe_filename: str,
    uploaded_at: datetime,
    prefix: str = "documents",
) -> str:
    normalized_prefix = prefix.strip("/")
    return (
        f"{normalized_prefix}/{user_id}/{uploaded_at:%Y}/{uploaded_at:%m}/"
        f"{document_id}-{safe_filename}"
    )


class VercelBlobStorage:
    def __init__(self, settings: Settings) -> None:
        self.prefix = settings.blob_prefix
        self.token = settings.blob_read_write_token
        self._client: Any | None = None

    def _get_client(self) -> Any:
        if self._client is None:
            from vercel.blob import AsyncBlobClient

            self._client = AsyncBlobClient(token=self.token)
        return self._client

    async def put_document(
        self,
        *,
        user_id: uuid.UUID,
        document_id: uuid.UUID,
        safe_filename: str,
        content: bytes,
        content_type: str,
        uploaded_at: datetime,
    ) -> StoredBlob:
        pathname = build_document_blob_path(
            user_id=user_id,
            document_id=document_id,
            safe_filename=safe_filename,
            uploaded_at=uploaded_at,
            prefix=self.prefix,
        )
        result = await self._get_client().put(
            pathname,
            content,
            access="private",
            content_type=content_type,
        )
        return StoredBlob(
            pathname=_blob_value(result, "pathname"),
            url=_blob_value(result, "url"),
            download_url=_blob_value(result, "download_url"),
            etag=_optional_blob_value(result, "etag"),
        )

    async def delete_documents(self, pathnames: Sequence[str]) -> None:
        if not pathnames:
            return

        delete = getattr(self._get_client(), "delete", None)
        if delete is None:
            from vercel.blob import delete as delete_blob

            delete = delete_blob

        result = delete(list(pathnames))
        if inspect.isawaitable(result):
            await result

    async def get_document_content(self, pathname: str) -> bytes:
        result = await self._get_client().get(pathname, access="private")
        content = _blob_value(result, "content")
        if not isinstance(content, bytes):
            raise RuntimeError("Blob response did not contain bytes content.")
        return content


def _blob_value(blob: object, key: str) -> Any:
    if isinstance(blob, dict):
        return blob[key]
    return getattr(blob, key)


def _optional_blob_value(blob: object, key: str) -> str | None:
    if isinstance(blob, dict):
        return blob.get(key)
    return getattr(blob, key, None)
