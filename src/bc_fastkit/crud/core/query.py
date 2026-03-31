from typing import Any, Dict, List, Optional, Tuple, Type

from sqlalchemy import or_
from sqlalchemy.orm import Query

from .typing import BaseModel, ModelType


def model_filter(q: Dict[str, Any], model: BaseModel) -> bool:
    for k, v in q.items():
        if v is None:
            return False

        val = getattr(model, k, None)

        if isinstance(v, dict):
            for op, op_val in v.items():
                if op == "between":
                    if not (op_val[0] <= val <= op_val[1]):
                        return False
                elif op == "le":
                    if not (val <= op_val):
                        return False
                elif op == "ge":
                    if not (val >= op_val):
                        return False
                elif op == "lt":
                    if not (val < op_val):
                        return False
                elif op == "gt":
                    if not (val > op_val):
                        return False
                elif op == "neq":
                    if isinstance(op_val, list):
                        if val in op_val:
                            return False
                    else:
                        if val == op_val:
                            return False
                elif op == "in":
                    if val not in op_val:
                        return False
                elif op == "not_in":
                    if val in op_val:
                        return False
                elif op == "eq":
                    if val != op_val:
                        return False
                # like/regexp are harder to eval in pure python reliably; skipped for model_filter
        else:
            if val != v:
                return False

    return True


def sql_filter(
    q: Dict[str, Any],
    query: Query,
    model: Type[ModelType],
    ignore_none=True,
    ignore_deleted=True,
) -> Query:
    for k, v in q.items():
        if v is None:
            continue

        if k in model.column_names:
            column = getattr(model, k)
            if isinstance(v, dict):
                for op, op_val in v.items():
                    if op == "between":
                        query = query.filter(column.between(*op_val))
                    elif op == "le":
                        query = query.filter(column <= op_val)
                    elif op == "ge":
                        query = query.filter(column >= op_val)
                    elif op == "lt":
                        query = query.filter(column < op_val)
                    elif op == "gt":
                        query = query.filter(column > op_val)
                    elif op == "neq":
                        query = query.filter(column != op_val)
                    elif op == "in":
                        query = query.filter(column.in_(op_val))
                    elif op == "not_in":
                        query = query.filter(column.not_in(op_val))
                    elif op == "regexp":
                        query = query.filter(
                            column.op("regexp")(uniform_regexp_string(op_val))
                        )
                    elif op == "complexregexp":
                        query = query.filter(column.op("regexp")(op_val))
                    elif op == "like":
                        query = query.filter(column.like(op_val))
                    elif op == "ilike":
                        query = query.filter(column.ilike(op_val))
                    elif op == "eq":
                        query = query.filter(column == op_val)
            else:
                if isinstance(v, str):
                    v = v.strip()
                query = query.filter(column == v)

    if model.is_fake_delete:
        delete_column = getattr(model, "is_deleted")
        if ignore_deleted:
            if ignore_none:
                query = query.filter(delete_column == 0)
            # outerjoin时显示
            else:
                query = query.filter(or_(delete_column == 0, delete_column.is_(None)))
    return query


def uniform_regexp_string(s: str):
    for char in ["\\", ".", "^", "$", "*", "+", "?", "{", "}", "[", "]", "(", ")", "|"]:
        s = s.replace(char, "\\" + char)
    return s


def sql_page_filter(
    q: Dict[str, Any],
    query: Query,
    model: Type[ModelType],
    skip: int,
    limit: int,
    order_by: Optional[List[Any]] = None,
) -> Tuple[List[Any], int]:
    order_by = order_by or [model.id.desc()]
    query = sql_filter(q=q, query=query, model=model)
    rs = query.order_by(*order_by).offset(skip).limit(limit).all()
    return rs, query.count()
