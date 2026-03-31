from typing import Any, Dict, List, Optional, Tuple, Type

from sqlalchemy import func, or_, select
from sqlalchemy.sql import Select

from .query import uniform_regexp_string
from .typing import ModelType


def async_sql_filter(
    q: Dict[str, Any],
    query: Select,
    model: Type[ModelType],
    ignore_none=True,
    ignore_deleted=True,
) -> Select:
    def _apply_op(column, value):
        if isinstance(value, dict):
            _query = None
            for op, op_val in value.items():
                if op == "between":
                    cond = column.between(*op_val)
                elif op == "le":
                    cond = column <= op_val
                elif op == "ge":
                    cond = column >= op_val
                elif op == "lt":
                    cond = column < op_val
                elif op == "gt":
                    cond = column > op_val
                elif op == "neq":
                    cond = column != op_val
                elif op == "in":
                    cond = column.in_(op_val)
                elif op == "not_in":
                    cond = column.not_in(op_val)
                elif op == "regexp":
                    cond = column.op("regexp")(uniform_regexp_string(op_val))
                elif op == "complexregexp":
                    cond = column.op("regexp")(op_val)
                elif op == "like":
                    cond = column.like(op_val)
                elif op == "ilike":
                    cond = column.ilike(op_val)
                elif op == "eq":
                    cond = column == op_val
                else:
                    continue
                _query = _query & cond if _query is not None else cond
            return _query
        else:
            if isinstance(value, str):
                value = value.strip()
            return column == value

    for k, v in q.items():
        if v is None:
            continue

        column = None
        if k in model.column_names:
            column = getattr(model, k)
        elif "." in k:
            parts = k.split(".")
            if parts[0] in model.column_names:
                col_name = parts[0]
                path = f"$.{'.'.join(parts[1:])}"
                # Use dedicated JSON extract and unquote for MySQL
                column = func.json_unquote(
                    func.json_extract(getattr(model, col_name), path)
                )

        if column is not None:
            cond = _apply_op(column, v)
            if cond is not None:
                query = query.filter(cond)

    if model.is_fake_delete:
        delete_column = getattr(model, "is_deleted")
        if ignore_deleted:
            if ignore_none:
                query = query.filter(delete_column == 0)
            else:
                query = query.filter(or_(delete_column == 0, delete_column.is_(None)))
    return query


async def async_sql_page_filter(
    db: Any,  # AsyncSession
    q: Dict[str, Any],
    query: Select,
    model: Type[ModelType],
    skip: int,
    limit: int,
    order_by: Optional[List[Any]] = None,
) -> Tuple[List[Any], int]:
    order_by = order_by or [model.id.desc()]
    query = async_sql_filter(q=q, query=query, model=model)

    # Get total count first
    count_stmt = select(func.count()).select_from(query.subquery())
    count_result = await db.execute(count_stmt)
    total = count_result.scalar() or 0

    # Get paginated results
    paginated_stmt = query.order_by(*order_by).offset(skip).limit(limit)
    res = await db.execute(paginated_stmt)
    data = res.scalars().all()

    return list(data), total
