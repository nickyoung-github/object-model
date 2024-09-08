from requests import Session, codes
from typing import Any

from . import DBContext
from .persistable import DBRecord
from ..json import dumps, loads


class HttpContext(DBContext):
    def __init__(self, base_url: str):
        super().__init__()
        self.__base_url = base_url
        self.__session = Session()

    def _execute_reads(self, reads: tuple[DBRecord, ...]) -> tuple[DBRecord, ...]:
        return self.__post("read", {"reads": reads})

    def _execute_writes(self,
                        writes: tuple[DBRecord, ...],
                        username: str,
                        hostname: str,
                        comment: str) -> tuple[DBRecord, ...]:
        return self.__post("write", {"writes": writes, "username": username, "hostname": hostname, "comment": ""})

    def __post(self, endpoint: str, request: dict[str, Any]):
        result = self.__session.post(f"{self.__base_url}/{endpoint}", data=dumps(request))
        if result.status_code == codes.ok:
            return tuple(DBRecord(**r) for r in loads(result.content))
        else:
            result.raise_for_status()
