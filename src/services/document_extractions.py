from typing import Any

from sqlalchemy import Text, cast, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.document import DocumentExtractionItem, UploadedDocument
from src.schemas.document import DocumentExtractionItemListResponse, DocumentExtractionItemResponse

EXTRACTION_ITEM_TYPES = {"person"}


class DocumentExtractionItemService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def list_items(
        self,
        *,
        item_type: str | None,
        search: str | None,
        limit: int,
        offset: int,
    ) -> DocumentExtractionItemListResponse:
        filters = [
            UploadedDocument.deleted_at.is_(None),
            DocumentExtractionItem.type.in_(EXTRACTION_ITEM_TYPES),
        ]
        if item_type:
            filters.append(DocumentExtractionItem.type == item_type)
        if search:
            pattern = f"%{search.strip()}%"
            filters.append(
                or_(
                    cast(DocumentExtractionItem.value_json, Text).ilike(pattern),
                    DocumentExtractionItem.evidence_text.ilike(pattern),
                    UploadedDocument.safe_filename.ilike(pattern),
                    UploadedDocument.original_filename.ilike(pattern),
                )
            )

        total = await self.session.scalar(
            select(func.count())
            .select_from(DocumentExtractionItem)
            .join(UploadedDocument, UploadedDocument.id == DocumentExtractionItem.document_id)
            .where(*filters)
        )
        result = await self.session.execute(
            select(DocumentExtractionItem, UploadedDocument.safe_filename)
            .join(UploadedDocument, UploadedDocument.id == DocumentExtractionItem.document_id)
            .where(*filters)
            .order_by(DocumentExtractionItem.created_at.desc(), DocumentExtractionItem.id.desc())
            .limit(limit)
            .offset(offset)
        )
        items = [
            extraction_item_response(item, document_filename=safe_filename)
            for item, safe_filename in result.all()
        ]
        return DocumentExtractionItemListResponse(
            items=items,
            total=total or 0,
            limit=limit,
            offset=offset,
        )


def extraction_item_response(
    item: DocumentExtractionItem,
    *,
    document_filename: str,
) -> DocumentExtractionItemResponse:
    return DocumentExtractionItemResponse(
        id=item.id,
        document_id=item.document_id,
        document_filename=document_filename,
        type=item.type,
        value_json=cast_value_json(item.value_json),
        confidence=item.confidence,
        source=item.source,
        evidence_text=item.evidence_text,
        page_start=item.page_start,
        page_end=item.page_end,
        char_start=item.char_start,
        char_end=item.char_end,
        created_at=item.created_at,
    )


def cast_value_json(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
