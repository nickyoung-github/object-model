from fastapi import FastAPI
from jsonschema import validate
from orjson import loads
from pydantic import BaseModel
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


class RegisterTypeRequest(BaseModel):
    name: str
    json_schema: dict


app = FastAPI()
db = MemoryStore()
schemas: dict[str, dict] = {}


@app.post("/read/")
async def read(request: ReadRequest) -> tuple[ObjectRecord, ...]:
    return db._execute_reads(request.reads)


@app.post("/write/")
async def write(request: WriteRequest) -> tuple[ObjectRecord, ...]:
    for record in request.writes:
        schema = schemas.get(record.object_type)
        if not schema:
            raise RuntimeError(f"Attempt to write unregistered type {record.object_type}")

        validate(schema=schema, instance=loads(record.object_contents))

    return db._execute_writes(request.writes, request.username, request.hostname, request.comment)


@app.post("/register_type/")
async def register_type(request: RegisterTypeRequest):
    schemas[request.name] = request.json_schema


if __name__ == "__main__":
    uvicorn.run(app)
