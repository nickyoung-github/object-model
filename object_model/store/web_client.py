from pydantic import BaseModel
from requests import Session, codes
from typing import Iterable

from .._json import dumps, loads

from .object_record import ObjectRecord
from .object_store import ObjectStore, ReadRequest, RegisterSchemaRequest, WriteRequest


class WebStoreClient(ObjectStore):
    def __init__(self, base_url: str):
        super().__init__(False, False)
        self.__base_url = base_url
        self.__session = Session()

    def _execute_reads(self, reads: ReadRequest) -> Iterable[ObjectRecord]:
        return self.__post("read", reads)

    def _execute_writes(self, writes: WriteRequest) -> Iterable[ObjectRecord]:
        return self.__post("write", writes)

    def register_schema(self, json_schema: RegisterSchemaRequest):
        self.__post("register", json_schema)

    def __post(self, endpoint: str, request: BaseModel):
        result = self.__session.post(f"{self.__base_url}/{endpoint}", data=dumps(request))
        if result.status_code == codes.ok:
            return tuple(ObjectRecord(**r) for r in loads(result.content))
        else:
            result.raise_for_status()
