from requests import Session, codes

from . import DBContext
from .persistable import DBRecord
from ..json import dumps, loads


class HttpContext(DBContext):
    def __init__(self, url: str):
        super().__init__()
        self.__url = url
        self.__session = Session()

    def _execute_reads(self, reads: tuple[DBRecord, ...]) -> tuple[DBRecord, ...]:
        request = {"reads": reads}
        result = self.__session.post(f"{self.__url}/read", data=dumps(request))
        if result.status_code == codes.ok:
            return tuple(DBRecord(**r) for r in loads(result.content))
        else:
            result.raise_for_status()

    def _execute_writes(self,
                        writes: tuple[DBRecord, ...],
                        username: str,
                        hostname: str,
                        comment: str) -> tuple[DBRecord, ...]:
        request = {"writes": writes, "username": username, "hostname": hostname, "comment": ""}
        result = self.__session.post(f"{self.__url}/write", data=dumps(request))
        if result.status_code == codes.ok:
            return tuple(DBRecord(**r) for r in loads(result.content))
        else:
            result.raise_for_status()
