from fastapi import FastAPI
from pydantic import BaseModel
from typing import Iterable
import uvicorn

from object_model.store.persistable import ObjectRecord
from object_model.store import MemoryStore


class ReadRequest(BaseModel):
    reads: tuple[ObjectRecord, ...]


class WriteRequest(BaseModel):
    writes: tuple[ObjectRecord, ...]
    username: str
    hostname: str
    comment: str


class RegisterSchemaRequest(BaseModel):
    name: str
    json_schema: dict


app = FastAPI()
db = MemoryStore()  # Use the actual DB here


@app.post("/read/")
async def read(request: ReadRequest) -> Iterable[ObjectRecord]:
    return db._execute_reads(request.reads)


@app.post("/write/")
async def write(request: WriteRequest) -> Iterable[ObjectRecord]:
    # ToDo: This isn't quite right - we should have a single schema, to avoid duplication of referenced types
    return db._execute_writes_with_check(request.writes, request.username, request.hostname, request.comment)


@app.post("/register/")
async def register(request: RegisterSchemaRequest):
    db.register_schema(request.name, request.json_schema)


if __name__ == "__main__":
    uvicorn.run(app)
