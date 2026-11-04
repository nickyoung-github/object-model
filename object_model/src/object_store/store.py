from typing import Any, Iterable, Protocol

from .persistable import Persistable
        

class ObjectStore(Protocol):
    def read(self, object_ids: Iterable[str]) -> dict[str, Persistable]:
        ...

    def write(self, objects: Iterable[Persistable]):
        ...
