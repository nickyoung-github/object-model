from itertools import chain
from typing import Iterable

from .object_record import ObjectRecord
from .object_store import ObjectStore, ReadRequest, RegisterSchemaRequest, WriteRequest


class UnionStore(ObjectStore):
    def __init__(self, stores: Iterable[ObjectStore]):
        assert stores

        self.__stores = tuple(stores)
        self.__write_store = self.__stores[0]

        super().__init__(self.__write_store.check_schema, self.__write_store.allow_temporary_types)

    def _execute_reads(self, reads: ReadRequest) -> Iterable[ObjectRecord]:
        # ToDo: make this parallel with async
        results = [store._execute_reads(reads) for store in self.__stores]
        grouped_results = {}
        ret = tuple()

        for r in chain.from_iterable(results):
            grouped_results.setdefault((r.object_id_type, r.object_id), []).append(r)

        # Find the most recent for each type/id
        for all_store_results in grouped_results.values():
            ret += (sorted(all_store_results, key=lambda i: (i.effective_time, i.entry_time))[-1],)

        return ret

    def _execute_writes(self, writes: WriteRequest) -> Iterable[ObjectRecord]:
        return self.__write_store._execute_writes(writes)

    def register_schema(self, request: RegisterSchemaRequest):
        self.__write_store.register_schema(request)
