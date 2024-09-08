from fastapi import FastAPI
import uvicorn

from object_model import BaseModel
from object_model.db.persistable import DBRecord
from object_model.db import SqliteContext

app = FastAPI()
db = SqliteContext()


class ReadRequest(BaseModel):
    reads: tuple[DBRecord, ...]


class WriteRequest(BaseModel):
    writes: tuple[DBRecord, ...]
    username: str
    hostname: str
    comment: str


@app.post("/read/")
async def read(request: ReadRequest) -> tuple[DBRecord, ...]:
    return db._execute_reads(request.reads)


@app.post("/write/")
async def write(request: WriteRequest) -> tuple[DBRecord, ...]:
    return db._execute_writes(request.writes, request.username, request.hostname, request.comment)


if __name__ == "__main__":
    uvicorn.run(app)
