import asyncio
import json
import re
import time
import unicodedata
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol

import structlog
from fastapi import BackgroundTasks, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.db.session import AsyncSessionLocal
from src.core.security import utcnow
from src.core.settings import Settings, get_settings
from src.models.document import (
    DocumentChunk,
    DocumentExtractionItem,
    DocumentProcessingRun,
    DocumentProcessingStatus,
    ParsedDocument,
    UploadedDocument,
)
from src.schemas.document import DocumentProcessingDetailsResponse, DocumentProcessingRunResponse
from src.services.blob_storage import BlobStorage, VercelBlobStorage
from src.services.document_chunking import DocumentChunker, PreparedChunk
from src.services.document_parsing import (
    DocumentOcrFailedError,
    DocumentOcrTimeoutError,
    DocumentOcrUnavailableError,
    DocumentNeedsOcrError,
    DocumentParser,
    ParsedDocumentContent,
    UnsupportedDocumentTypeError,
)

EXTRACTION_VERSION = "openai-structured-v1"
logger = structlog.get_logger(__name__)

ProcessingStage = Literal["blob_read", "parse", "chunk", "embed", "extract", "persist"]


@dataclass(frozen=True)
class ExtractedItem:
    type: str
    value_json: dict
    confidence: float | None
    source: str
    evidence_text: str | None = None
    chunk_id: uuid.UUID | None = None
    page_start: int | None = None
    page_end: int | None = None
    char_start: int | None = None
    char_end: int | None = None
    metadata: dict | None = None


class DocumentEmbeddingClient(Protocol):
    async def embed_texts(self, texts: Sequence[str]) -> list[list[float]]:
        ...


class DocumentExtractionClient(Protocol):
    async def extract_items(
        self,
        *,
        document_text: str,
        chunks: Sequence[DocumentChunk],
    ) -> list[ExtractedItem]:
        ...


class DocumentProcessingScheduler(Protocol):
    def schedule_documents(self, document_ids: Sequence[uuid.UUID]) -> None:
        ...


class FastAPIDocumentProcessingScheduler:
    def __init__(self, background_tasks: BackgroundTasks) -> None:
        self.background_tasks = background_tasks

    def schedule_documents(self, document_ids: Sequence[uuid.UUID]) -> None:
        for document_id in document_ids:
            logger.info(
                "document_processing.scheduled",
                document_id=str(document_id),
                stage="schedule",
                status="scheduled",
            )
            self.background_tasks.add_task(process_uploaded_document, document_id)


class DocumentProcessingQueueService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def queue_document(self, *, document_id: uuid.UUID) -> UploadedDocument:
        document = await get_active_document(self.session, document_id)
        ensure_document_can_be_processed(document)
        document.processing_status = DocumentProcessingStatus.QUEUED
        document.processing_error = None
        document.processing_error_code = None
        document.processing_error_stage = None
        document.processing_started_at = None
        document.processing_completed_at = None
        await self.session.commit()
        await self.session.refresh(document)
        return document

    async def get_processing_details(
        self,
        *,
        document_id: uuid.UUID,
    ) -> DocumentProcessingDetailsResponse:
        document = await get_active_document(self.session, document_id)
        latest_run = await get_latest_processing_run(self.session, document_id)
        return processing_details_response(document, latest_run)


class OpenAIEmbeddingClient:
    def __init__(self, settings: Settings) -> None:
        self.api_key = settings.openai_api_key
        self.model = settings.openai_embedding_model
        self.dimensions = settings.openai_embedding_dimensions
        self._client = None

    def _get_client(self):
        if self._client is None:
            from openai import AsyncOpenAI

            self._client = AsyncOpenAI(api_key=self.api_key)
        return self._client

    async def embed_texts(self, texts: Sequence[str]) -> list[list[float]]:
        if not texts:
            return []
        response = await self._get_client().embeddings.create(
            model=self.model,
            input=list(texts),
            dimensions=self.dimensions,
        )
        return [item.embedding for item in response.data]


class ExtractionItemSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["person"]
    title: str | None = Field(default=None)
    summary: str | None = Field(default=None)
    person_name: str | None = Field(default=None)
    raw_value: str | None = Field(default=None)
    confidence: float | None = Field(default=None, ge=0, le=1)
    evidence_text: str | None = Field(default=None)
    page_start: int | None = Field(default=None)
    page_end: int | None = Field(default=None)
    char_start: int | None = Field(default=None)
    char_end: int | None = Field(default=None)


class ExtractionResultSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: list[ExtractionItemSchema]


class OpenAIDocumentExtractionClient:
    def __init__(self, settings: Settings) -> None:
        self.api_key = settings.openai_api_key
        self.model = settings.openai_extraction_model
        self.concurrency = settings.openai_extraction_concurrency
        self._client = None

    def _get_client(self):
        if self._client is None:
            from openai import AsyncOpenAI

            self._client = AsyncOpenAI(api_key=self.api_key)
        return self._client

    async def extract_items(
        self,
        *,
        document_text: str,
        chunks: Sequence[DocumentChunk],
    ) -> list[ExtractedItem]:
        del document_text
        if not chunks:
            return []

        semaphore = asyncio.Semaphore(self.concurrency)

        async def extract_chunk(chunk: DocumentChunk) -> list[ExtractedItem]:
            async with semaphore:
                return await self._extract_chunk_items(chunk)

        chunk_results = await asyncio.gather(*(extract_chunk(chunk) for chunk in chunks))
        return post_process_extracted_items(
            [item for chunk_items in chunk_results for item in chunk_items]
        )

    async def _extract_chunk_items(self, chunk: DocumentChunk) -> list[ExtractedItem]:
        chunk_context = {
            "id": str(chunk.id),
            "page_start": chunk.page_start,
            "page_end": chunk.page_end,
            "char_start": chunk.char_start,
            "char_end": chunk.char_end,
        }
        prompt = (
            "Extract useful structured information from this Ukrainian university document chunk. "
            "Return only facts grounded in the provided text. Only extract this item type: "
            "person. "
            "Use person for named people only, preferably with role/title in title or summary. "
            "Do not extract departments, кафедри/chairs, institutes/institutions, dean offices, offices, laboratories, centers, "
            "contacts, deadlines, programs, forms, requirements, rules, partner universities, companies, or program names. "
            "Use exact evidence text where possible. Do not infer facts from outside the chunk.\n\n"
            f"Chunk citation metadata:\n{json.dumps(chunk_context, ensure_ascii=False)}\n\n"
            f"Chunk text:\n{chunk.content}"
        )
        response = await self._get_client().responses.parse(
            model=self.model,
            input=[
                {
                    "role": "system",
                    "content": "You extract structured data for a university document search system.",
                },
                {"role": "user", "content": prompt},
            ],
            text_format=ExtractionResultSchema,
        )
        parsed = response.output_parsed
        return [
            ExtractedItem(
                type=item.type,
                value_json=extraction_value_json(item),
                confidence=item.confidence,
                source="openai",
                evidence_text=item.evidence_text,
                chunk_id=chunk.id,
                page_start=item.page_start if item.page_start is not None else chunk.page_start,
                page_end=item.page_end if item.page_end is not None else chunk.page_end,
                char_start=item.char_start if item.char_start is not None else chunk.char_start,
                char_end=item.char_end if item.char_end is not None else chunk.char_end,
            )
            for item in parsed.items
        ]


class DocumentProcessingService:
    def __init__(
        self,
        session: AsyncSession,
        *,
        blob_storage: BlobStorage,
        embedding_client: DocumentEmbeddingClient,
        extraction_client: DocumentExtractionClient,
        parser: DocumentParser | None = None,
        chunker: DocumentChunker | None = None,
        settings: Settings | None = None,
    ) -> None:
        self.session = session
        self.blob_storage = blob_storage
        self.embedding_client = embedding_client
        self.extraction_client = extraction_client
        self.parser = parser or DocumentParser()
        self.chunker = chunker or DocumentChunker()
        self.settings = settings or get_settings()

    async def queue_document(self, *, document_id: uuid.UUID) -> UploadedDocument:
        return await DocumentProcessingQueueService(self.session).queue_document(
            document_id=document_id,
        )

    async def process_document(self, *, document_id: uuid.UUID) -> None:
        document = await get_active_document(self.session, document_id)
        document_metrics = document_base_metrics(document)
        processing_run = await self._start_processing_run(document)
        processing_run_id_for_update = processing_run.id
        processing_run_id = str(processing_run.id)
        run_start = time.perf_counter()
        stage_metrics: dict[str, dict[str, Any]] = {}
        log = self._document_logger(document, processing_run_id=processing_run_id)
        log.info("document_processing.started", stage="start", status="started")

        try:
            content = await self._run_stage(
                "blob_read",
                log,
                lambda: self._read_blob_content(document),
                stage_metrics=stage_metrics,
                processing_run_id=processing_run_id_for_update,
                document_metrics=document_metrics,
                metrics_from_result=lambda content: {"bytes_read": len(content)},
            )
            parsed = await self._run_stage(
                "parse",
                log,
                lambda: self.parser.parse(
                    filename=document.safe_filename,
                    file_extension=document.file_extension,
                    content=content,
                ),
                stage_metrics=stage_metrics,
                processing_run_id=processing_run_id_for_update,
                document_metrics=document_metrics,
                metrics_from_result=parsed_document_metrics,
            )
            prepared_chunks = await self._run_stage(
                "chunk",
                log,
                lambda: self.chunker.chunk(parsed),
                stage_metrics=stage_metrics,
                processing_run_id=processing_run_id_for_update,
                document_metrics=document_metrics,
                metrics_from_result=prepared_chunk_metrics,
            )
            embeddings = await self._run_stage(
                "embed",
                log,
                lambda: self.embedding_client.embed_texts(
                    [chunk.content for chunk in prepared_chunks]
                ),
                stage_metrics=stage_metrics,
                processing_run_id=processing_run_id_for_update,
                document_metrics=document_metrics,
                metrics_from_result=lambda embeddings: {
                    "embedding_count": len(embeddings),
                    "embedding_model": self.settings.openai_embedding_model,
                },
                chunk_count=len(prepared_chunks),
            )
            if len(embeddings) != len(prepared_chunks):
                raise ProcessingFailure(
                    status=DocumentProcessingStatus.FAILED,
                    error_code="embedding_failed",
                    stage="embed",
                    safe_message="Document embedding failed.",
                    original=RuntimeError("Embedding count does not match chunk count."),
                )
            chunk_models = build_chunk_models(
                document_id=document.id,
                chunks=prepared_chunks,
                embeddings=embeddings,
                embedding_model=self.settings.openai_embedding_model,
            )
            extraction_items = await self._run_stage(
                "extract",
                log,
                lambda: self.extraction_client.extract_items(
                    document_text=parsed.text,
                    chunks=chunk_models,
                ),
                stage_metrics=stage_metrics,
                processing_run_id=processing_run_id_for_update,
                document_metrics=document_metrics,
                metrics_from_result=lambda extraction_items: {
                    "extraction_count": len(extraction_items),
                    "extraction_model": self.settings.openai_extraction_model,
                    "extraction_request_count": len(chunk_models),
                    "extraction_concurrency": self.settings.openai_extraction_concurrency,
                },
                chunk_count=len(chunk_models),
                embedding_count=len(embeddings),
            )
            await self._run_stage(
                "persist",
                log,
                lambda: self._replace_artifacts(
                    document=document,
                    parsed=parsed,
                    chunks=chunk_models,
                    extraction_items=extraction_items,
                ),
                stage_metrics=stage_metrics,
                processing_run_id=processing_run_id_for_update,
                document_metrics=document_metrics,
                metrics_from_result=lambda _: {"persisted": True},
                chunk_count=len(chunk_models),
                embedding_count=len(embeddings),
                extraction_count=len(extraction_items),
            )
            total_duration_ms = elapsed_ms(run_start)
            summary_metrics = build_summary_metrics(
                document_metrics=document_metrics,
                stage_metrics=stage_metrics,
                total_duration_ms=total_duration_ms,
            )
            await self._mark_run_terminal(
                processing_run,
                status=DocumentProcessingStatus.COMPLETED,
                stage_metrics=stage_metrics,
                summary_metrics=summary_metrics,
                total_duration_ms=total_duration_ms,
            )
            log.info(
                "document_processing.completed",
                stage="complete",
                status="completed",
                total_duration_ms=total_duration_ms,
                chunk_count=len(chunk_models),
                embedding_count=len(embeddings),
                extraction_count=len(extraction_items),
            )
        except ProcessingFailure as exc:
            total_duration_ms = elapsed_ms(run_start)
            await self.session.rollback()
            log.warning(
                "document_processing.failed",
                stage=exc.stage,
                status="failed",
                total_duration_ms=total_duration_ms,
                error_code=exc.error_code,
                error_type=exc.original.__class__.__name__ if exc.original else exc.__class__.__name__,
                exc_info=exc.original if should_log_traceback(exc) else None,
            )
            await self._mark_terminal(
                document_id=document_id,
                processing_run_id=processing_run_id_for_update,
                status=exc.status,
                error=exc.safe_message,
                error_code=exc.error_code,
                error_stage=exc.stage,
                stage_metrics=stage_metrics,
                summary_metrics=build_summary_metrics(
                    document_metrics=document_metrics,
                    stage_metrics=stage_metrics,
                    total_duration_ms=total_duration_ms,
                ),
                total_duration_ms=total_duration_ms,
            )
        except Exception as exc:
            total_duration_ms = elapsed_ms(run_start)
            await self.session.rollback()
            log.exception(
                "document_processing.failed",
                stage="unknown",
                status="failed",
                total_duration_ms=total_duration_ms,
                error_code="unexpected_failed",
                error_type=exc.__class__.__name__,
            )
            await self._mark_terminal(
                document_id=document_id,
                processing_run_id=processing_run_id_for_update,
                status=DocumentProcessingStatus.FAILED,
                error="Document processing failed unexpectedly.",
                error_code="unexpected_failed",
                error_stage="unknown",
                stage_metrics=stage_metrics,
                summary_metrics=build_summary_metrics(
                    document_metrics=document_metrics,
                    stage_metrics=stage_metrics,
                    total_duration_ms=total_duration_ms,
                ),
                total_duration_ms=total_duration_ms,
            )

    async def _read_blob_content(self, document: UploadedDocument) -> bytes:
        if not document.storage_key:
            raise MissingDocumentStorageKeyError("Document has no blob storage key.")
        return await self.blob_storage.get_document_content(document.storage_key)

    async def _run_stage(
        self,
        stage: ProcessingStage,
        log: Any,
        operation,
        *,
        stage_metrics: dict[str, dict[str, Any]],
        processing_run_id: uuid.UUID,
        document_metrics: dict[str, Any],
        metrics_from_result=None,
        **fields,
    ):
        started_at = utcnow()
        start = time.perf_counter()
        stage_metrics[stage] = build_stage_started_metric(
            started_at=started_at,
            **safe_metric_fields(fields),
        )
        await self._persist_run_progress(
            processing_run_id=processing_run_id,
            stage_metrics=stage_metrics,
            document_metrics=document_metrics,
            total_duration_ms=None,
        )
        log.info(
            "document_processing.stage.started",
            stage=stage,
            status="started",
            **fields,
        )
        try:
            result = operation()
            if hasattr(result, "__await__"):
                result = await result
        except Exception as exc:
            completed_at = utcnow()
            duration_ms = elapsed_ms(start)
            failure = classify_processing_error(stage, exc)
            metric_fields = safe_metric_fields(fields)
            stage_metrics[stage] = build_stage_metric(
                status="failed",
                started_at=started_at,
                completed_at=completed_at,
                duration_ms=duration_ms,
                error_code=failure.error_code,
                error_type=exc.__class__.__name__,
                **metric_fields,
            )
            if stage != "persist":
                await self._persist_run_progress(
                    processing_run_id=processing_run_id,
                    stage_metrics=stage_metrics,
                    document_metrics=document_metrics,
                    total_duration_ms=None,
                )
            log.warning(
                "document_processing.stage.failed",
                stage=stage,
                status="failed",
                duration_ms=duration_ms,
                error_code=failure.error_code,
                error_type=exc.__class__.__name__,
                exc_info=exc if should_log_traceback(failure) else None,
                **fields,
            )
            raise failure from exc
        completed_at = utcnow()
        duration_ms = elapsed_ms(start)
        result_metrics = safe_metric_fields(metrics_from_result(result) if metrics_from_result else {})
        log_fields = {**fields, **result_metrics}
        stage_metrics[stage] = build_stage_metric(
            status="completed",
            started_at=started_at,
            completed_at=completed_at,
            duration_ms=duration_ms,
            **safe_metric_fields(log_fields),
        )
        await self._persist_run_progress(
            processing_run_id=processing_run_id,
            stage_metrics=stage_metrics,
            document_metrics=document_metrics,
            total_duration_ms=None,
        )
        log.info(
            "document_processing.stage.completed",
            stage=stage,
            status="completed",
            duration_ms=duration_ms,
            **log_fields,
        )
        return result

    @staticmethod
    def _document_logger(document: UploadedDocument, *, processing_run_id: str):
        return logger.bind(
            document_id=str(document.id),
            processing_run_id=processing_run_id,
            filename=document.safe_filename,
            file_extension=document.file_extension,
            size_bytes=document.size_bytes,
            storage_key_present=bool(document.storage_key),
        )

    async def _start_processing_run(self, document: UploadedDocument) -> DocumentProcessingRun:
        started_at = utcnow()
        document.processing_status = DocumentProcessingStatus.PROCESSING
        document.processing_error = None
        document.processing_error_code = None
        document.processing_error_stage = None
        document.processing_started_at = started_at
        document.processing_completed_at = None
        processing_run = DocumentProcessingRun(
            document_id=document.id,
            status=DocumentProcessingStatus.PROCESSING,
            started_at=started_at,
            stage_metrics_json={},
            summary_metrics_json=build_summary_metrics(
                document_metrics=document_base_metrics(document),
                stage_metrics={},
                total_duration_ms=None,
            ),
        )
        self.session.add(processing_run)
        await self.session.commit()
        await self.session.refresh(processing_run)
        return processing_run

    async def _persist_run_progress(
        self,
        *,
        processing_run_id: uuid.UUID,
        stage_metrics: dict[str, dict[str, Any]],
        document_metrics: dict[str, Any],
        total_duration_ms: int | None,
    ) -> None:
        run = await self.session.get(DocumentProcessingRun, processing_run_id)
        if run is None:
            return
        run.status = DocumentProcessingStatus.PROCESSING
        run.stage_metrics_json = dict(stage_metrics)
        run.summary_metrics_json = build_summary_metrics(
            document_metrics=document_metrics,
            stage_metrics=stage_metrics,
            total_duration_ms=total_duration_ms,
        )
        await self.session.commit()

    async def _mark_run_terminal(
        self,
        processing_run: DocumentProcessingRun,
        *,
        status: DocumentProcessingStatus,
        stage_metrics: dict[str, dict[str, Any]],
        summary_metrics: dict[str, Any],
        total_duration_ms: int,
        error: str | None = None,
        error_code: str | None = None,
        error_stage: str | None = None,
    ) -> None:
        run = await self.session.get(DocumentProcessingRun, processing_run.id)
        if run is None:
            return
        run.status = status
        run.completed_at = utcnow()
        run.total_duration_ms = total_duration_ms
        run.error_code = error_code
        run.error_stage = error_stage
        run.error_message = error
        run.stage_metrics_json = dict(stage_metrics)
        run.summary_metrics_json = summary_metrics
        await self.session.commit()

    async def _mark_terminal(
        self,
        *,
        document_id: uuid.UUID,
        processing_run_id: uuid.UUID,
        status: DocumentProcessingStatus,
        error: str | None,
        error_code: str | None = None,
        error_stage: str | None = None,
        stage_metrics: dict[str, dict[str, Any]],
        summary_metrics: dict[str, Any],
        total_duration_ms: int,
    ) -> None:
        completed_at = utcnow()
        persisted_document = await self.session.get(UploadedDocument, document_id)
        persisted_run = await self.session.get(DocumentProcessingRun, processing_run_id)
        if persisted_document is None or persisted_run is None:
            return
        persisted_document.processing_status = status
        persisted_document.processing_error = error
        persisted_document.processing_error_code = error_code
        persisted_document.processing_error_stage = error_stage
        persisted_document.processing_completed_at = completed_at
        persisted_run.status = status
        persisted_run.completed_at = completed_at
        persisted_run.total_duration_ms = total_duration_ms
        persisted_run.error_code = error_code
        persisted_run.error_stage = error_stage
        persisted_run.error_message = error
        persisted_run.stage_metrics_json = dict(stage_metrics)
        persisted_run.summary_metrics_json = summary_metrics
        await self.session.commit()

    async def _replace_artifacts(
        self,
        *,
        document: UploadedDocument,
        parsed: ParsedDocumentContent,
        chunks: list[DocumentChunk],
        extraction_items: list[ExtractedItem],
    ) -> None:
        await self.session.execute(
            delete(DocumentExtractionItem).where(DocumentExtractionItem.document_id == document.id)
        )
        await self.session.execute(delete(DocumentChunk).where(DocumentChunk.document_id == document.id))
        await self.session.execute(
            delete(ParsedDocument).where(ParsedDocument.document_id == document.id)
        )
        self.session.add(
            ParsedDocument(
                document_id=document.id,
                raw_text=parsed.text,
                language="ukrainian",
                parser_version=parsed.parser_version,
                page_count=len(parsed.pages),
                metadata_json=parsed.metadata,
                outline_json=[
                    {
                        "title": section.title,
                        "page_start": section.page_start,
                        "char_start": section.char_start,
                        "char_end": section.char_end,
                    }
                    for section in parsed.sections
                ],
            )
        )
        self.session.add_all(chunks)
        self.session.add_all(
            [
                DocumentExtractionItem(
                    document_id=document.id,
                    chunk_id=item.chunk_id,
                    type=item.type,
                    value_json=item.value_json,
                    confidence=item.confidence,
                    source=item.source,
                    evidence_text=item.evidence_text,
                    page_start=item.page_start,
                    page_end=item.page_end,
                    char_start=item.char_start,
                    char_end=item.char_end,
                    metadata_json=item.metadata or {},
                )
                for item in extraction_items
            ]
        )
        document.processing_status = DocumentProcessingStatus.COMPLETED
        document.processing_error = None
        document.processing_error_code = None
        document.processing_error_stage = None
        document.processing_completed_at = utcnow()
        document.parser_version = parsed.parser_version
        document.extraction_version = EXTRACTION_VERSION
        await self.session.commit()


class MissingDocumentStorageKeyError(RuntimeError):
    pass


class ProcessingFailure(Exception):
    def __init__(
        self,
        *,
        status: DocumentProcessingStatus,
        error_code: str,
        stage: ProcessingStage,
        safe_message: str,
        original: Exception | None = None,
    ) -> None:
        super().__init__(safe_message)
        self.status = status
        self.error_code = error_code
        self.stage = stage
        self.safe_message = safe_message
        self.original = original


def build_chunk_models(
    *,
    document_id: uuid.UUID,
    chunks: Sequence[PreparedChunk],
    embeddings: Sequence[list[float]],
    embedding_model: str,
) -> list[DocumentChunk]:
    return [
        DocumentChunk(
            id=uuid.uuid4(),
            document_id=document_id,
            chunk_id=uuid.uuid4(),
            chunk_index=chunk.chunk_index,
            title=chunk.title,
            section_path=chunk.section_path,
            content=chunk.content,
            fts_text=normalize_fts_text(chunk.content),
            search_vector=func.to_tsvector("simple", normalize_fts_text(chunk.content)),
            embedding=embeddings[index],
            embedding_model=embedding_model,
            token_count=chunk.token_count,
            page_start=chunk.page_start,
            page_end=chunk.page_end,
            char_start=chunk.char_start,
            char_end=chunk.char_end,
            metadata_json=chunk.metadata,
            is_active=True,
        )
        for index, chunk in enumerate(chunks)
    ]


def normalize_fts_text(text: str) -> str:
    normalized = unicodedata.normalize("NFKC", text).lower()
    return " ".join(normalized.split())


def safe_error(exc: Exception) -> str:
    return str(exc)[:1000] or exc.__class__.__name__


def classify_processing_error(stage: ProcessingStage, exc: Exception) -> ProcessingFailure:
    if isinstance(exc, ProcessingFailure):
        return exc
    if isinstance(exc, MissingDocumentStorageKeyError):
        return ProcessingFailure(
            status=DocumentProcessingStatus.FAILED,
            error_code="missing_storage_key",
            stage=stage,
            safe_message="Document has no blob storage key.",
            original=exc,
        )
    if isinstance(exc, UnsupportedDocumentTypeError):
        return ProcessingFailure(
            status=DocumentProcessingStatus.UNSUPPORTED,
            error_code="unsupported_file_type",
            stage=stage,
            safe_message=str(exc),
            original=exc,
        )
    if isinstance(exc, DocumentNeedsOcrError):
        error_code = "ocr_unavailable" if isinstance(exc, DocumentOcrUnavailableError) else "needs_ocr"
        return ProcessingFailure(
            status=DocumentProcessingStatus.NEEDS_OCR,
            error_code=error_code,
            stage=stage,
            safe_message=str(exc),
            original=exc,
        )
    if isinstance(exc, DocumentOcrTimeoutError):
        return ProcessingFailure(
            status=DocumentProcessingStatus.FAILED,
            error_code="ocr_timeout",
            stage=stage,
            safe_message="Document OCR timed out.",
            original=exc,
        )
    if isinstance(exc, DocumentOcrFailedError):
        return ProcessingFailure(
            status=DocumentProcessingStatus.FAILED,
            error_code="ocr_failed",
            stage=stage,
            safe_message="Document OCR failed.",
            original=exc,
        )

    error_code_by_stage = {
        "blob_read": "blob_read_failed",
        "parse": "parse_failed",
        "chunk": "parse_failed",
        "embed": "embedding_failed",
        "extract": extraction_error_code(exc),
        "persist": "persistence_failed",
    }
    safe_message_by_code = {
        "blob_read_failed": "Document file could not be read from storage.",
        "parse_failed": "Document parsing failed.",
        "embedding_failed": "Document embedding failed.",
        "extraction_schema_invalid": "Structured extraction schema is invalid.",
        "extraction_provider_failed": "Structured extraction failed.",
        "persistence_failed": "Document processing results could not be saved.",
    }
    error_code = error_code_by_stage[stage]
    return ProcessingFailure(
        status=DocumentProcessingStatus.FAILED,
        error_code=error_code,
        stage=stage,
        safe_message=safe_message_by_code[error_code],
        original=exc,
    )


def extraction_error_code(exc: Exception) -> str:
    message = str(exc).lower()
    if "invalid_json_schema" in message or "additionalproperties" in message:
        return "extraction_schema_invalid"
    return "extraction_provider_failed"


def should_log_traceback(failure: ProcessingFailure) -> bool:
    return failure.error_code not in {
        "missing_storage_key",
        "unsupported_file_type",
        "needs_ocr",
        "ocr_unavailable",
    }


def elapsed_ms(start: float) -> int:
    return round((time.perf_counter() - start) * 1000)


def build_stage_started_metric(*, started_at, **fields) -> dict[str, Any]:
    metric: dict[str, Any] = {
        "status": "started",
        "started_at": started_at.isoformat(),
    }
    metric.update(safe_metric_fields(fields))
    return metric


def build_stage_metric(
    *,
    status: str,
    started_at,
    completed_at,
    duration_ms: int,
    error_code: str | None = None,
    error_type: str | None = None,
    **fields,
) -> dict[str, Any]:
    metric: dict[str, Any] = {
        "status": status,
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "duration_ms": duration_ms,
    }
    if error_code is not None:
        metric["error_code"] = error_code
    if error_type is not None:
        metric["error_type"] = error_type
    metric.update(safe_metric_fields(fields))
    return metric


def safe_metric_fields(fields: dict[str, Any]) -> dict[str, Any]:
    safe_types = (str, int, float, bool, type(None))
    return {key: value for key, value in fields.items() if isinstance(value, safe_types)}


def parsed_document_metrics(parsed: ParsedDocumentContent) -> dict[str, Any]:
    metrics = {
        "page_count": len(parsed.pages),
        "section_count": len(parsed.sections),
        "text_char_count": len(parsed.text),
        "parser_version": parsed.parser_version,
    }
    for key in (
        "ocr_engine",
        "ocr_languages",
        "ocr_page_count",
        "ocr_duration_ms",
        "ocr_timeout",
        "ocr_fallback",
    ):
        value = parsed.metadata.get(key)
        if value is not None:
            metrics[key] = value
    return metrics


def prepared_chunk_metrics(chunks: Sequence[PreparedChunk]) -> dict[str, Any]:
    return {
        "chunk_count": len(chunks),
        "token_count_total": sum(chunk.token_count for chunk in chunks),
    }


def document_base_metrics(document: UploadedDocument) -> dict[str, Any]:
    return {
        "file_extension": document.file_extension,
        "size_bytes": document.size_bytes,
    }


def build_summary_metrics(
    *,
    document_metrics: dict[str, Any],
    stage_metrics: dict[str, dict[str, Any]],
    total_duration_ms: int | None,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "file_extension": document_metrics["file_extension"],
        "size_bytes": document_metrics["size_bytes"],
        "total_duration_ms": total_duration_ms,
    }
    copy_metric_value(summary, "bytes_read", stage_metrics, ("blob_read",))
    copy_metric_value(summary, "page_count", stage_metrics, ("parse",))
    copy_metric_value(summary, "section_count", stage_metrics, ("parse",))
    copy_metric_value(summary, "text_char_count", stage_metrics, ("parse",))
    copy_metric_value(summary, "parser_version", stage_metrics, ("parse",))
    copy_metric_value(summary, "ocr_engine", stage_metrics, ("parse",))
    copy_metric_value(summary, "ocr_languages", stage_metrics, ("parse",))
    copy_metric_value(summary, "ocr_page_count", stage_metrics, ("parse",))
    copy_metric_value(summary, "ocr_duration_ms", stage_metrics, ("parse",))
    copy_metric_value(summary, "chunk_count", stage_metrics, ("chunk", "embed", "extract", "persist"))
    copy_metric_value(summary, "token_count_total", stage_metrics, ("chunk",))
    copy_metric_value(summary, "embedding_count", stage_metrics, ("embed", "extract", "persist"))
    copy_metric_value(summary, "embedding_model", stage_metrics, ("embed",))
    copy_metric_value(summary, "extraction_count", stage_metrics, ("extract", "persist"))
    copy_metric_value(summary, "extraction_model", stage_metrics, ("extract",))
    copy_metric_value(summary, "extraction_request_count", stage_metrics, ("extract",))
    copy_metric_value(summary, "extraction_concurrency", stage_metrics, ("extract",))
    return summary


def copy_metric_value(
    summary: dict[str, Any],
    key: str,
    stage_metrics: dict[str, dict[str, Any]],
    stages: tuple[str, ...],
) -> None:
    for stage in stages:
        value = stage_metrics.get(stage, {}).get(key)
        if value is not None:
            summary[key] = value
            return


def extraction_value_json(item: ExtractionItemSchema) -> dict[str, Any]:
    excluded_fields = {
        "type",
        "confidence",
        "evidence_text",
        "page_start",
        "page_end",
        "char_start",
        "char_end",
    }
    return {
        key: value
        for key, value in item.model_dump().items()
        if key not in excluded_fields and value is not None
    }


def post_process_extracted_items(items: Sequence[ExtractedItem]) -> list[ExtractedItem]:
    processed = []
    for item in items:
        normalized = normalize_extracted_item(item)
        if normalized is not None:
            processed.append(normalized)
    return deduplicate_extracted_items(processed)


def normalize_extracted_item(item: ExtractedItem) -> ExtractedItem | None:
    if item.type != "person":
        return None
    return normalize_person_item(item)


def normalize_person_item(item: ExtractedItem) -> ExtractedItem | None:
    value_json = dict(item.value_json)
    person_name = first_string_value(value_json, ("person_name", "title", "raw_value"))
    if not person_name:
        return None
    value_json["person_name"] = person_name
    return replace_extracted_item(item, value_json=value_json)


def first_string_value(value_json: dict, keys: Sequence[str]) -> str | None:
    for key in keys:
        value = value_json.get(key)
        if isinstance(value, str) and value.strip():
            return normalize_spacing(value)
    return None


def normalize_spacing(value: str) -> str:
    return " ".join(value.split())


def replace_extracted_item(
    item: ExtractedItem,
    *,
    type: str | None = None,
    value_json: dict | None = None,
) -> ExtractedItem:
    return ExtractedItem(
        type=type or item.type,
        value_json=value_json if value_json is not None else item.value_json,
        confidence=item.confidence,
        source=item.source,
        evidence_text=item.evidence_text,
        chunk_id=item.chunk_id,
        page_start=item.page_start,
        page_end=item.page_end,
        char_start=item.char_start,
        char_end=item.char_end,
        metadata=item.metadata,
    )


def deduplicate_extracted_items(items: Sequence[ExtractedItem]) -> list[ExtractedItem]:
    deduplicated: dict[str, ExtractedItem] = {}
    for item in items:
        key = extracted_item_dedupe_key(item)
        existing = deduplicated.get(key)
        if existing is None:
            deduplicated[key] = item
            continue
        existing_confidence = existing.confidence if existing.confidence is not None else -1.0
        item_confidence = item.confidence if item.confidence is not None else -1.0
        if item_confidence > existing_confidence:
            deduplicated[key] = item
    return list(deduplicated.values())


def extracted_item_dedupe_key(item: ExtractedItem) -> str:
    if item.type == "person":
        person_name = first_string_value(item.value_json, ("person_name", "title", "raw_value"))
        return f"person:{normalization_key(person_name or '')}"

    value_key = json.dumps(item.value_json, sort_keys=True, ensure_ascii=False)
    evidence_key = normalize_spacing((item.evidence_text or "").lower())
    return f"{item.type}:{value_key}:{evidence_key}"


def normalization_key(value: str) -> str:
    normalized = value.lower().replace("’", "'").replace("ʼ", "'")
    normalized = re.sub(r"\([^)]*\)", "", normalized)
    normalized = re.sub(r"[^\w\s']", " ", normalized, flags=re.UNICODE)
    return normalize_spacing(normalized)


def ensure_document_can_be_processed(document: UploadedDocument) -> None:
    if document.storage_key:
        return
    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail=(
            "Document cannot be processed because it has no blob storage key. "
            "Re-upload the file so it is stored in Vercel Blob."
        ),
    )


async def get_active_document(session: AsyncSession, document_id: uuid.UUID) -> UploadedDocument:
    result = await session.execute(
        select(UploadedDocument).where(
            UploadedDocument.id == document_id,
            UploadedDocument.deleted_at.is_(None),
        )
    )
    document = result.scalar_one_or_none()
    if document is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Document was not found.",
        )
    return document


async def get_latest_processing_run(
    session: AsyncSession,
    document_id: uuid.UUID,
) -> DocumentProcessingRun | None:
    result = await session.execute(
        select(DocumentProcessingRun)
        .where(DocumentProcessingRun.document_id == document_id)
        .order_by(DocumentProcessingRun.started_at.desc(), DocumentProcessingRun.created_at.desc())
        .limit(1)
    )
    return result.scalar_one_or_none()


def processing_details_response(
    document: UploadedDocument,
    latest_run: DocumentProcessingRun | None,
) -> DocumentProcessingDetailsResponse:
    return DocumentProcessingDetailsResponse(
        document_id=document.id,
        processing_status=document.processing_status,
        processing_error=document.processing_error,
        processing_error_code=document.processing_error_code,
        processing_error_stage=document.processing_error_stage,
        processing_started_at=document.processing_started_at,
        processing_completed_at=document.processing_completed_at,
        latest_run=processing_run_response(latest_run) if latest_run else None,
    )


def processing_run_response(processing_run: DocumentProcessingRun) -> DocumentProcessingRunResponse:
    return DocumentProcessingRunResponse(
        id=processing_run.id,
        document_id=processing_run.document_id,
        status=processing_run.status,
        started_at=processing_run.started_at,
        completed_at=processing_run.completed_at,
        total_duration_ms=processing_run.total_duration_ms,
        error_code=processing_run.error_code,
        error_stage=processing_run.error_stage,
        error_message=processing_run.error_message,
        stage_metrics=processing_run.stage_metrics_json or {},
        summary_metrics=processing_run.summary_metrics_json or {},
        created_at=processing_run.created_at,
        updated_at=processing_run.updated_at,
    )


async def process_uploaded_document(document_id: uuid.UUID) -> None:
    settings = get_settings()
    try:
        async with AsyncSessionLocal() as session:
            service = DocumentProcessingService(
                session,
                blob_storage=VercelBlobStorage(settings),
                embedding_client=OpenAIEmbeddingClient(settings),
                extraction_client=OpenAIDocumentExtractionClient(settings),
                settings=settings,
            )
            await service.process_document(document_id=document_id)
    except Exception as exc:
        logger.exception(
            "document_processing.background_task_failed",
            document_id=str(document_id),
            stage="background_task",
            status="failed",
            error_code="background_task_failed",
            error_type=exc.__class__.__name__,
        )
