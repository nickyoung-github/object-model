import narwhals as nw
from narwhals.dtypes import DType
from pathlib import Path
from types import NoneType

from anyschema._dependencies import is_dataclass
from anyschema.adapters import dataclass_adapter
from anyschema.exceptions import UnsupportedDTypeError
from anyschema.parsers import AnnotatedStep, ForwardRefStep, PyTypeStep, UnionTypeStep, make_pipeline
from anyschema.parsers._base import ParserStep
from anyschema.parsers.annotated_types import AnnotatedTypesStep
from anyschema.parsers.pydantic import PydanticTypeStep
from anyschema.typing import FieldConstraints, FieldMetadata, FieldType

from .persistable import Persistable


class DataclassTypeStep(ParserStep):
    def parse(
        self,
        input_type: FieldType,
        constraints: FieldConstraints,  # noqa: ARG002
        metadata: FieldMetadata,
    ) -> DType | None:
        if is_dataclass(input_type):
            return nw.Struct(
                [
                    nw.Field(
                        name=field_name,
                        dtype=self.pipeline.parse(field_info, field_constraints, field_metadata, strict=True),
                    )
                    for field_name, field_info, field_constraints, field_metadata in dataclass_adapter(input_type)
                ]
            )


class _UnionTypeStep(UnionTypeStep):
    # Replaces the anyschema one, so we can handle references

    def _parse_union(self, union: tuple[FieldType, ...]) -> FieldType:
        if len(union) != 2:  # noqa: PLR2004
            msg = "Union with more than two types is not supported."
            raise UnsupportedDTypeError(msg)

        field0, field1 = union

        if field0 is not NoneType and field1 is not NoneType:
            msg = "Union with mixed types is not supported."

            for f in (field0, field1):
                if f is not Path and not issubclass(f, Persistable):
                    raise UnsupportedDTypeError(msg)

            return str

        return field1 if field0 is NoneType else field0


PARSER_STEPS = (
    ForwardRefStep(),
    _UnionTypeStep(),
    AnnotatedStep(),
    AnnotatedTypesStep(),
    DataclassTypeStep(),
    PydanticTypeStep(),
    PyTypeStep()
)

PIPELINE = make_pipeline(PARSER_STEPS)
