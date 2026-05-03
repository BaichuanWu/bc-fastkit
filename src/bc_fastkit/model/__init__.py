import hashlib
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence, Set, cast

import pandas as pd
from sqlalchemy import (
    BIGINT,
    BINARY,
    DATETIME,
    DECIMAL,
    TEXT,
    VARCHAR,
    Column,
    Computed,
    DateTime,
    Index,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.mysql import INTEGER, TINYINT
from sqlalchemy.orm import Mapped, MappedColumn, as_declarative, declared_attr
from sqlalchemy.sql import func
from sqlalchemy.sql.sqltypes import JSON

from ..common.typing import DATE_FORMAT, DATETIME_FORMAT, D, date_re, datetime_re
from ..common.uitls import classproperty
from .column import (
    DefaultDecimalColumn,
    DefaultIdColumn,
    DefaultJsonColumn,
    DefaultTextColumn,
    DefaultTimeColumn,
    DefaultTypeColumn,
    NotNullColumn,
)


def to_camel(string: str) -> str:
    parts = string.split("_")
    return parts[0] + "".join(word.capitalize() for word in parts[1:])


class MappingMixin:
    """
    Mixin to automatically provide name properties for models with state/typ mappings.
    Example: If a model has STATE_NAME_MAPPING, it will get a .state_name property.
    """

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        for mapping_attr in cls.get_mappings():
            field = mapping_attr[: -len("_NAME_MAPPING")].lower()
            setattr(
                cls, f"{field}_name", cls._make_mapping_property(field, mapping_attr)
            )

    @classmethod
    def _make_mapping_property(cls, field: str, mapping_attr: str):
        @property
        def prop(self) -> str:
            val = getattr(self, field, None)
            mapping = getattr(cls, mapping_attr, {})
            return mapping.get(val, f"未知({val})" if val is not None else "N/A")

        return prop

    @classmethod
    def get_mappings(cls) -> Dict[str, Dict[int, str]]:
        """Extract all mapping dictionaries from the class."""
        mappings = {}
        for attr in dir(cls):
            if attr.endswith("_NAME_MAPPING") and isinstance(getattr(cls, attr), dict):
                mappings[attr] = getattr(cls, attr)
        return mappings


@as_declarative()
class BaseModel(MappingMixin):
    FAKE_DELETE_UK_SUFFIX = "_DELETED_"

    id: Mapped[int] = NotNullColumn(BIGINT, primary_key=True)
    create_time = NotNullColumn(
        DateTime,
        server_default=text(str(func.current_timestamp())),
        comment="数据创建时间",
    )
    update_time = NotNullColumn(
        DateTime,
        server_default=text(
            " ON UPDATE ".join([str(func.current_timestamp())] * 2)
        ),  # server_onupdate don't work in mysql
        comment="数据更新时间",
    )

    def __eq__(self, o: object) -> bool:
        return isinstance(o, self.__class__) and self.id != 0 and self.id == o.id

    def __hash__(self) -> int:
        return int(
            hashlib.md5(f"{self.__class__.__name__}:{self.id}".encode()).hexdigest(), 16
        )

    @declared_attr
    def __tablename__(cls: Any) -> Any:
        if cls.__name__[-5:] != "Model":
            raise ValueError("model class name should end up with Model")
        name_list = re.findall(r"[A-Z][a-z\d]*", cls.__name__)[:-1]
        return "_".join(name_list).lower()

    @classproperty
    def schema_name(cls: Any) -> str:
        return cls.__name__[:-5]

    @declared_attr
    def column_names(cls) -> Mapped[Set[str]]:
        names = set()
        attrs = dir(cls)
        for attr in attrs:
            if "column_names" not in attr:
                col = getattr(cls, attr)
                if isinstance(col, (Column, MappedColumn)):
                    names.add(attr)
        return names  # type: ignore

    @classproperty
    def creatable_column_names(cls) -> Set[str]:
        return cls.column_names - {"id", "create_time", "update_time"}

    @classproperty
    def immutable_column_names(cls) -> Set[str]:
        return {"id", "cno", "create_time", "update_time"}

    @classproperty
    def mutable_column_names(cls) -> Set[str]:
        return cls.column_names - cls.immutable_column_names

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}:(id:{self.id};cno:{getattr(self, 'cno', '')})"
        )

    @classproperty
    def is_real_delete(cls):
        return True

    @classproperty
    def is_fake_delete(cls):
        return hasattr(cls, "is_deleted")

    @classproperty
    def unique_column_names(cls) -> List[str]:
        return [c for c in cls.column_names if getattr(cls, c).unique]

    # @classmethod
    # def add_relation(
    #     cls,
    #     attr_name: str,
    #     peer_model: "BaseModel",
    #     column_name: str,
    #     mapping_model: "BaseMappingModel" = None,
    #     peer_column_name: str = "",
    # ):
    #     cls.relation_dict = getattr(cls, "relation_dict", {})
    #     cls.relation_dict[attr_name] = Relation(
    #         attr_name=attr_name,
    #         entity_model=cls,
    #         peer_model=peer_model,
    #         column_name=column_name,
    #         mapping_model=mapping_model,
    #         peer_column_name=peer_column_name,
    #     )

    @classmethod
    def transfer_column_value(cls, attr_name, value):
        attr = getattr(cls, attr_name)
        if attr:
            python_type = get_column_python_type(attr)
            if python_type == Decimal:
                return value and Decimal(value)
            elif value and python_type in (date, datetime) and isinstance(value, str):
                if datetime_re.match(value):
                    return datetime.strptime(value, DATETIME_FORMAT)
                elif date_re.match(value):
                    return datetime.strptime(value, DATE_FORMAT)
        return value

    def to_dict(self):
        return {c: getattr(self, c) for c in self.column_names}

    @classmethod
    def to_md(
        cls,
        objs: List["BaseModel"],
        exclude: Optional[Sequence[str]] = None,
        include: Optional[Sequence[str]] = None,
    ) -> str:
        columns: Set[str] = (
            set(include) if include else set(cast(Set[str], cls.column_names))
        )
        if exclude:
            columns -= set(exclude)
        df = pd.DataFrame([obj.to_dict() for obj in objs])
        df = df[list(columns)]
        return df.to_markdown(index=False, tablefmt="github")

    def copy(self, other=None):
        return self.__class__(
            **{
                c: getattr(other, c, None) or getattr(self, c)
                for c in self.column_names
            }
        )

    @classmethod
    def from_dict(cls, obj_in: D):
        return cls(
            **{k: cls.transfer_column_value(k, obj_in.get(k)) for k in cls.column_names}  # type: ignore
        )

    @property
    def key(self) -> int:
        return self.id


def get_column_python_type(column: Column) -> Optional[type]:
    python_type: Optional[type] = None
    if column.name == "files":
        pass
    if isinstance(column.type, JSON):
        arg = getattr(column.server_default, "arg", None)
        default_text = getattr(arg, "text", "")
        # 通过 server_default 推断 JSON 字段的具体类型
        if default_text.startswith("(json_array"):
            python_type = List
        elif default_text.startswith("(json_object"):
            python_type = Dict[str, Any]
        else:
            python_type = List | Dict[str, Any]  # type: ignore
    elif hasattr(column.type, "impl"):
        impl = column.type.impl  # type: ignore
        if hasattr(impl, "python_type"):
            python_type = impl.python_type
    elif hasattr(column.type, "python_type"):
        python_type = column.type.python_type
    return python_type


__all__ = [
    "BaseModel",
    "NotNullColumn",
    "DefaultIdColumn",
    "DefaultTypeColumn",
    "DefaultTextColumn",
    "DefaultDecimalColumn",
    "DefaultJsonColumn",
    "VARCHAR",
    "TEXT",
    "INTEGER",
    "TINYINT",
    "get_column_python_type",
    "UniqueConstraint",
    "Computed",
    "BINARY",
    "declared_attr",
    "classproperty",
    "DECIMAL",
    "DefaultTimeColumn",
    "DATETIME",
    "Index",
]
