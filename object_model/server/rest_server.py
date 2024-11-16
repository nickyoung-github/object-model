from fastapi import FastAPI
import uvicorn

from object_model import BaseModel
from object_model.store.persistable import ObjectRecord
from object_model.store.sql import SqlStore

app = FastAPI()
db = SqlStore()


class ReadRequest(BaseModel):
    reads: tuple[ObjectRecord, ...]


class WriteRequest(BaseModel):
    writes: tuple[ObjectRecord, ...]
    username: str
    hostname: str
    comment: str


@app.post("/read/")
async def read(request: ReadRequest) -> tuple[ObjectRecord, ...]:
    return db._execute_reads(request.reads)


@app.post("/write/")
async def write(request: WriteRequest) -> tuple[ObjectRecord, ...]:
    return db._execute_writes(request.writes, request.username, request.hostname, request.comment)


if __name__ == "__main__":
    uvicorn.run(app)
