from pathlib import Path
from typing import Annotated, Any, TypeVar

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema
from pydantic_core.core_schema import (
    SerializationInfo,
    ValidationInfo,
    with_info_before_validator_function,
    wrap_serializer_function_ser_schema,
)

from .object_store import ReferenceResolver
from .persistable import Persistable


class _Reference:
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: type[Persistable], handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        schema = handler(source_type)

        def resolver(info: SerializationInfo | ValidationInfo) -> ReferenceResolver:
            ret = (info.context or {}).get("resolver")
            if ret is None:
                raise RuntimeError("Need a context to dump or validate Reference")

            return ret

        def validate(value: Any, info: ValidationInfo) -> Any:
            if isinstance(value, source_type):
                return value
            elif not isinstance(value, str):
                raise TypeError(
                    f"Expected an object id or {source_type!s}, got {type(value)!s}"
                )
            return resolver(info).get(Path(value))

        def serialise(value: Persistable, _serialiser, info: SerializationInfo) -> Any:
            try:
                resolver(info).write(value)
                return str(value.object_id)
            except RuntimeError:
                return value

        return with_info_before_validator_function(
            validate,
            schema,
            serialization=wrap_serializer_function_ser_schema(
                serialise, schema=schema, info_arg=True
            ),
        )


__REFERENCE = _Reference()
T = TypeVar("T", bound=type[Persistable])
Reference = Annotated[T | Path, __REFERENCE]
