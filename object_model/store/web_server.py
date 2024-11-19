from fastapi import FastAPI
from typing import Iterable
import uvicorn

from object_model.store.object_record import ObjectRecord
from object_model.store.object_store import ReadRequest, RegisterSchemaRequest, WriteRequest
from object_model.store import MemoryStore


app = FastAPI()
db = MemoryStore()  # Use the actual DB here


@app.post("/read/")
async def read(request: ReadRequest) -> Iterable[ObjectRecord]:
    return db._execute_reads(request)


@app.post("/write/")
async def write(request: WriteRequest) -> Iterable[ObjectRecord]:
    return db._execute_writes_with_check(request)


@app.post("/register/")
async def register(request: RegisterSchemaRequest):
    db.register_schema(request)


if __name__ == "__main__":
    uvicorn.run(app)
