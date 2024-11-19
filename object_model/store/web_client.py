from requests import Session, codes
from typing import Any

from .._json import dumps, loads, schema

from .store_types import ObjectRecord
from .persistable import PersistableMixin
from .store import ObjectStore


class WebStoreClient(ObjectStore):
    def __init__(self, base_url: str):
        super().__init__(False)
        self.__base_url = base_url
        self.__session = Session()

    def _execute_reads(self, reads: tuple[ObjectRecord, ...]) -> tuple[ObjectRecord, ...]:
        return self.__post("read", {"reads": reads})

    def _execute_writes(self,
                        writes: tuple[ObjectRecord, ...],
                        username: str,
                        hostname: str,
                        comment: str) -> tuple[ObjectRecord, ...]:
        return self.__post("write", {"writes": writes, "username": username, "hostname": hostname, "comment": ""})

    def register_type(self, typ: type[PersistableMixin]):
        self.__post("register_type", {"name": typ.object_type, "schema": schema(typ)})

    def __post(self, endpoint: str, request: dict[str, Any]):
        result = self.__session.post(f"{self.__base_url}/{endpoint}", data=dumps(request))
        if result.status_code == codes.ok:
            return tuple(ObjectRecord(**r) for r in loads(result.content))
        else:
            result.raise_for_status()
