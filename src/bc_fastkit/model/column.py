import json
from datetime import datetime
from decimal import Decimal
from functools import partial
from typing import Callable, TypeVar

from sqlalchemy import BIGINT, DATETIME, TEXT, text
from sqlalchemy.dialects.mysql import TINYINT
from sqlalchemy.dialects.mysql.types import DECIMAL
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql.sqltypes import JSON

NotNullColumn: Callable[..., Mapped] = partial(mapped_column, nullable=False)


def transfer2json_default(data):
    d = (
        json.dumps(data)
        .replace("[", "json_array(")
        .replace("]", ")")
        .replace("{", "json_object(")
        .replace("}", ")")
    )
    return text(f"({d})")


def DefaultJsonColumn(server_default, **kwargs) -> Mapped[dict]:
    return mapped_column(
        JSON,
        nullable=False,
        server_default=transfer2json_default(server_default),
        **kwargs,
    )


DefaultIdColumn: Callable[..., Mapped[int]] = partial(
    mapped_column,
    BIGINT,
    index=True,
    nullable=False,
    server_default="0",
)

DefaultTypeColumn: Callable[..., Mapped[int]] = partial(
    mapped_column, TINYINT, nullable=False, server_default="0"
)
DefaultTextColumn: Callable[..., Mapped[str]] = partial(
    mapped_column, TEXT, nullable=False, server_default=""
)

DefaultDecimalColumn: Callable[..., Mapped[Decimal]] = partial(
    mapped_column, DECIMAL(20, 8), nullable=False, server_default="0.00000000"
)
DefaultTimeColumn: Callable[..., Mapped[datetime]] = partial(
    mapped_column, DATETIME, nullable=False, server_default="'1900-01-01 00:00:00'"
)

T = TypeVar("T")
