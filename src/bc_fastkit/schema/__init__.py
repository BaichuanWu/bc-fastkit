# type: ignore

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Any, Container, Dict, List, Optional, Tuple, Type, Union

from humps import camel
from pydantic import BaseModel, ConfigDict, PlainSerializer, create_model

from ..common.typing import DATE_FORMAT, DATETIME_FORMAT
from ..model import get_column_python_type

IGNORE_SUFFIX = " 00:00:00"

USED_TYPE_JSON = "json"

FDecimal = Annotated[
    Decimal,
    PlainSerializer(
        lambda x: float(Decimal(x).quantize(Decimal("0.0000")).normalize()),
        return_type=float,
        when_used=USED_TYPE_JSON,
    ),
]


def datetime_parser(t: datetime):
    rs = t.strftime(DATETIME_FORMAT)
    if rs.endswith(IGNORE_SUFFIX):
        return rs[: -len(IGNORE_SUFFIX)]
    return rs


class BaseSchema(BaseModel):
    model_config = ConfigDict(
        alias_generator=camel.case,
        populate_by_name=True,
        json_encoders={
            datetime: datetime_parser,
            date: lambda v: v.strftime(DATE_FORMAT),
        },
        from_attributes=True,
    )


class FileSchema(BaseSchema):
    name: str
    url: str


class QueryResponseSchema(BaseSchema):
    data_source: List[Any]
    total: Optional[int]
    query: Optional[Any]
    update_time: Optional[datetime]
    message: str = ""


class CRUSchema:
    def __init__(self, C: Type, U: Type, R: Type) -> None:
        self.C = C
        self.U = U
        self.R = R

    @property
    def QR(self) -> Type:
        if not hasattr(self, "_QR"):
            self._QR = create_model(
                f"tQuery{self.R.__name__}",
                __base__=QueryResponseSchema,
                data_source=(List[self.R], []),
            )
        return self._QR

    @QR.setter
    def QR(self, value: Type):
        self._QR = value


class CRUItemSchema(CRUSchema):
    def __init__(
        self, C: Type, U: Type, R: Type, ItemC: Type, ItemU: Type, ItemR: Type
    ) -> None:
        super().__init__(C, U, R)
        self.ItemC = ItemC
        self.ItemU = ItemU
        self.ItemR = ItemR

    @property
    def ItemQR(self) -> Type:
        if not hasattr(self, "_ItemQR"):
            self._ItemQR = create_model(
                f"Query{self.ItemR.__name__}",
                __base__=QueryResponseSchema,
                data_source=(List[self.ItemR], []),
            )
        return self._ItemQR

    @ItemQR.setter
    def ItemQR(self, value: Type):
        self._ItemQR = value


def create_schema_by_model(
    name_: str,
    db_model: Type,
    *,
    base: Type = BaseSchema,
    exclude: Container[str] = None,
    include: Container[str] = None,
    required: Container[str] = None,
    with_property: bool = False,
    **_fields,
) -> Type[BaseModel]:
    fields = {}
    for attr in db_model.column_names:
        if (include and attr not in include) or attr in (exclude or []):
            continue
        column = getattr(db_model, attr)
        python_type = get_column_python_type(column)
        assert python_type, f"Could not infer python_type for {column}"
        if python_type in [datetime, date]:
            python_type = Union[datetime, date]
        if python_type is Decimal:
            python_type = FDecimal
        if attr in (required or []):
            fields[attr] = (python_type, ...)
        else:
            fields[attr] = (Optional[python_type], None)
    if with_property:
        for attr in dir(db_model):
            if attr in (exclude or []):
                continue
            prop = getattr(db_model, attr)
            if isinstance(prop, property):
                annotations = prop.fget.__annotations__
                r_attr = "return"
                if r_attr not in annotations:
                    raise ValueError(f"未声明的返回类型:{db_model}->{attr}")
                fields[attr] = (Optional[annotations[r_attr]], None)
    for k, v in _fields.items():
        if not isinstance(v, Tuple):
            raise ValueError(f"错误的field参数:{db_model}->{k}: {v}")
    fields.update(_fields)
    pydantic_model = create_model(name_, __base__=base, **fields)
    return pydantic_model


def create_default_cru_schema(
    db_model: Type,
    *,
    c_base: Type = BaseSchema,
    u_base: Type = BaseSchema,
    r_base: Type = BaseSchema,
    c_exclude: List[str] = None,
    u_exclude: List[str] = None,
    r_exclude: List[str] = None,
    c_required: List[str] = None,
    u_required: List[str] = None,
    r_required: List[str] = None,
    c_include: List[str] = None,
    u_include: List[str] = None,
    r_include: List[str] = None,
    c_fields: Dict[str, Tuple[Type, Any]] = None,
    u_fields: Dict[str, Tuple[Type, Any]] = None,
    r_fields: Dict[str, Tuple[Type, Any]] = None,
) -> CRUSchema:
    create_schema = create_schema_by_model(
        db_model=db_model,
        base=c_base,
        exclude=["id", "create_time", "update_time", "is_deleted"] + (c_exclude or []),
        include=c_include,
        name_=f"DefaultCreate{db_model.schema_name}Schema",
        required=c_required,
        **(c_fields or {}),
    )
    update_schema = create_schema_by_model(
        db_model=db_model,
        base=u_base,
        exclude=["create_time", "update_time", "is_deleted"] + (u_exclude or []),
        include=(u_include and ["id"] + u_include) or None,
        required=(u_required and ["id"] + u_required) or ["id"],
        name_=f"DefaultUpdate{db_model.schema_name}Schema",
        **(u_fields or {}),
    )
    response_schema = create_schema_by_model(
        db_model=db_model,
        base=r_base,
        exclude=["is_deleted"] + (r_exclude or []),
        include=r_include,
        name_=f"Default{db_model.schema_name}ResponseSchema",
        required=r_required,
        with_property=True,
        **(r_fields or {}),
    )
    return CRUSchema(create_schema, update_schema, response_schema)


def create_item_cru_schema(
    db_model: Type,
    *,
    c_base: Type = BaseSchema,
    u_base: Type = BaseSchema,
    r_base: Type = BaseSchema,
    c_exclude: List[str] = None,
    u_exclude: List[str] = None,
    r_exclude: List[str] = None,
    c_required: List[str] = None,
    u_required: List[str] = None,
    r_required: List[str] = None,
    c_include: List[str] = None,
    u_include: List[str] = None,
    r_include: List[str] = None,
    c_fields: Dict[str, Tuple[Type, Any]] = None,
    u_fields: Dict[str, Tuple[Type, Any]] = None,
    r_fields: Dict[str, Tuple[Type, Any]] = None,
    item_c_base: Type = BaseSchema,
    item_u_base: Type = BaseSchema,
    item_r_base: Type = BaseSchema,
    item_c_exclude: List[str] = None,
    item_u_exclude: List[str] = None,
    item_r_exclude: List[str] = None,
    item_c_required: List[str] = None,
    item_u_required: List[str] = None,
    item_r_required: List[str] = None,
    item_c_include: List[str] = None,
    item_u_include: List[str] = None,
    item_r_include: List[str] = None,
    item_c_fields: Dict[str, Tuple[Type, Any]] = None,
    item_u_fields: Dict[str, Tuple[Type, Any]] = None,
    item_r_fields: Dict[str, Tuple[Type, Any]] = None,
    schema_typ: Type = CRUItemSchema,
) -> CRUItemSchema:
    assert db_model.ITEM_MODEL
    item_schema = create_default_cru_schema(
        db_model.ITEM_MODEL,
        c_base=item_c_base,
        u_base=item_u_base,
        r_base=item_r_base,
        c_exclude=item_c_exclude,
        u_exclude=item_u_exclude,
        r_exclude=item_r_exclude,
        c_required=item_c_required,
        u_required=item_u_required,
        r_required=item_r_required,
        c_include=item_c_include,
        u_include=item_u_include,
        r_include=item_r_include,
        c_fields=item_c_fields,
        u_fields={**(item_u_fields or {}), "id": (Optional[int], None)},
        r_fields=item_r_fields,
    )
    schema = create_default_cru_schema(
        db_model,
        c_base=c_base,
        u_base=u_base,
        r_base=r_base,
        c_exclude=c_exclude,
        u_exclude=u_exclude,
        r_exclude=r_exclude,
        c_required=c_required,
        u_required=u_required,
        r_required=r_required,
        c_include=c_include,
        u_include=u_include,
        r_include=r_include,
        c_fields={**(c_fields or {}), "items": (Optional[List[item_schema.C]], None)},
        u_fields={**(u_fields or {}), "items": (Optional[List[item_schema.U]], None)},
        r_fields={**(r_fields or {}), "items": (List[item_schema.R], [])},
    )
    return schema_typ(
        schema.C, schema.U, schema.R, item_schema.C, item_schema.U, item_schema.R
    )
