from requests import Session, codes
from typing import Any

from . import ObjectResult
from .persistable import ObjectRecord
from .._json import dumps, loads


class HttpStoreClient(ObjectResult):
    def __init__(self, base_url: str):
        super().__init__()
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

    def __post(self, endpoint: str, request: dict[str, Any]):
        result = self.__session.post(f"{self.__base_url}/{endpoint}", data=dumps(request))
        if result.status_code == codes.ok:
            return tuple(ObjectRecord(**r) for r in loads(result.content))
        else:
            result.raise_for_status()
