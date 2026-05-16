import re
from dataclasses import dataclass

from src.services.document_parsing import ParsedDocumentContent, ParsedPage


@dataclass(frozen=True)
class PreparedChunk:
    chunk_index: int
    title: str
    section_path: str | None
    content: str
    token_count: int
    page_start: int | None
    page_end: int | None
    char_start: int
    char_end: int
    metadata: dict


@dataclass(frozen=True)
class TextUnit:
    text: str
    section_path: str | None
    char_start: int
    char_end: int
    page_start: int | None
    page_end: int | None


class DocumentChunker:
    def __init__(self, *, target_tokens: int = 800, overlap_tokens: int = 100) -> None:
        self.target_tokens = target_tokens
        self.overlap_tokens = overlap_tokens

    def chunk(self, document: ParsedDocumentContent) -> list[PreparedChunk]:
        units = split_into_units(document)
        chunks: list[PreparedChunk] = []
        current: list[TextUnit] = []
        current_tokens = 0

        for unit in units:
            unit_tokens = count_tokens(unit.text)
            if current and current_tokens + unit_tokens > self.target_tokens:
                chunks.append(self._build_chunk(len(chunks), current))
                current = overlap_units(current, self.overlap_tokens)
                current_tokens = sum(count_tokens(item.text) for item in current)
            current.append(unit)
            current_tokens += unit_tokens

        if current:
            chunks.append(self._build_chunk(len(chunks), current))

        return chunks

    @staticmethod
    def _build_chunk(index: int, units: list[TextUnit]) -> PreparedChunk:
        content = "\n".join(unit.text for unit in units).strip()
        section_path = next((unit.section_path for unit in units if unit.section_path), None)
        return PreparedChunk(
            chunk_index=index,
            title=section_path or first_line(content),
            section_path=section_path,
            content=content,
            token_count=count_tokens(content),
            page_start=min((unit.page_start for unit in units if unit.page_start is not None), default=None),
            page_end=max((unit.page_end for unit in units if unit.page_end is not None), default=None),
            char_start=min(unit.char_start for unit in units),
            char_end=max(unit.char_end for unit in units),
            metadata={"unit_count": len(units)},
        )


def split_into_units(document: ParsedDocumentContent) -> list[TextUnit]:
    units: list[TextUnit] = []
    section_path: str | None = None
    for match in re.finditer(r"[^\n]+", document.text):
        text = match.group(0).strip()
        if not text:
            continue
        if is_heading(text):
            section_path = first_line(text)
        page_start, page_end = page_span_for_chars(
            document.pages,
            char_start=match.start(),
            char_end=match.end(),
        )
        units.append(
            TextUnit(
                text=text,
                section_path=section_path,
                char_start=match.start(),
                char_end=match.end(),
                page_start=page_start,
                page_end=page_end,
            )
        )
    return units


def overlap_units(units: list[TextUnit], overlap_tokens: int) -> list[TextUnit]:
    if overlap_tokens <= 0:
        return []
    selected: list[TextUnit] = []
    total = 0
    for unit in reversed(units):
        selected.append(unit)
        total += count_tokens(unit.text)
        if total >= overlap_tokens:
            break
    return list(reversed(selected))


def page_span_for_chars(
    pages: list[ParsedPage],
    *,
    char_start: int,
    char_end: int,
) -> tuple[int | None, int | None]:
    matching = [
        page.page_number
        for page in pages
        if page.char_start <= char_end and page.char_end >= char_start
    ]
    if not matching:
        return None, None
    return min(matching), max(matching)


def is_heading(text: str) -> bool:
    return bool(re.match(r"^\d+(?:\.\d+)*\.?\s+\S.+$", first_line(text)))


def count_tokens(text: str) -> int:
    return len(re.findall(r"\S+", text))


def first_line(text: str) -> str:
    return text.strip().splitlines()[0][:500] if text.strip() else ""
