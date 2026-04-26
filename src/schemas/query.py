from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    query: str = Field(min_length=3)


class QueryResponse(BaseModel):
    route: str
    answer: str
    sources: list[str] = []
