import json
from functools import partial
from typing import Generic, TypeVar

from pydantic import computed_field
from sqlalchemy import TEXT, Column, text
from sqlalchemy.dialects.mysql import INTEGER, TINYINT
from sqlalchemy.dialects.mysql.types import DECIMAL
from sqlalchemy.sql.sqltypes import JSON

NotNullColumn = partial(Column, nullable=False)


def transfer2json_default(data):
    d = (
        json.dumps(data)
        .replace("[", "json_array(")
        .replace("]", ")")
        .replace("{", "json_object(")
        .replace("}", ")")
    )
    return text(f"({d})")


def DefaultJsonColumn(server_default, **kwargs):
    return Column(
        JSON,
        nullable=False,
        server_default=transfer2json_default(server_default),
        **kwargs,
    )


DefaultIdColumn = partial(
    Column, INTEGER(unsigned=True), index=True, nullable=False, server_default="0"
)

DefaultTypeColumn = partial(Column, TINYINT, nullable=False, server_default="0")
DefaultTextColumn = partial(Column, TEXT, nullable=False, server_default="")

DefaultDecimalColumn = partial(
    Column, DECIMAL(20, 8), nullable=False, server_default="'0.00000000'"
)

T = TypeVar("T")


class ExtraField(Generic[T]):
    def __init__(self, default=None):
        self.default = default
        self.private_name = ""

    def __set_name__(self, owner, name: str):
        self.private_name = f"_{name}"
        # 注册 PrivateAttr

        # 定义 getter/setter
        def getter(inst) -> T | None:
            return getattr(inst, self.private_name, None)

        def setter(inst, value: T):
            setattr(inst, self.private_name, value)

        # 挂 computed_field property
        prop = property(getter, setter)
        setattr(owner, name, computed_field(prop))

        # 从注解里移除，避免进数据库
        if "__annotations__" in owner.__dict__:
            owner.__annotations__.pop(name, None)

    def __get__(self, instance, owner) -> T | None:
        # 仅调试用（正常情况下会被 property 替换）
        if instance is None:
            return
        return getattr(instance, self.private_name, None)

    def __set__(self, instance, value: T):
        setattr(instance, self.private_name, value)
