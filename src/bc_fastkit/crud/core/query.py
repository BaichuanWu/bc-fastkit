from typing import Any, Dict, List, Optional, Tuple, Type

from sqlalchemy import or_
from sqlalchemy.orm import Query

from .typing import BaseModel, ModelType


def model_filter(q: Dict[str, Any], model: BaseModel) -> bool:
    for k, v in q.items():
        if v is None:
            return False
        if k.endswith("_between"):
            return v[0] <= getattr(model, k[: -len("_between")]) <= v[1]
        elif k.endswith("_le"):
            return getattr(model, k[: -len("_le")]) <= v
        elif k.endswith("_ge"):
            return getattr(model, k[: -len("_ge")]) >= v
        elif k.endswith("_lt"):
            return getattr(model, k[: -len("_lt")]) < v
        elif k.endswith("_gt"):
            return getattr(model, k[: -len("_gt")]) > v
        elif k.endswith("_neq"):
            if isinstance(v, list):
                return getattr(model, k[: -len("_neq")]) not in v
            else:
                return getattr(model, k[: -len("_neq")]) != v
        elif isinstance(v, list):
            return getattr(model, k) in v
        else:
            return getattr(model, k) == v
    return True


# TODO 扩充 q 比如支持 or 查询
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
        is_between = False
        is_neq = False
        is_le = False
        is_ge = False
        is_lt = False
        is_gt = False
        is_regexp = False
        is_complex_regexp = False
        is_like = False
        if k.endswith("_between"):
            is_between = True
            k = k[: -len("_between")]
        elif k.endswith("_neq"):
            is_neq = True
            k = k[: -len("_neq")]
        elif k.endswith("_le"):
            is_le = True
            k = k[: -len("_le")]
        elif k.endswith("_ge"):
            is_ge = True
            k = k[: -len("_ge")]
        elif k.endswith("_lt"):
            is_lt = True
            k = k[: -len("_lt")]
        elif k.endswith("_gt"):
            is_gt = True
            k = k[: -len("_gt")]
        elif k.endswith("_regexp"):
            is_regexp = True
            k = k[: -len("_regexp")]
        elif k.endswith("_complexregexp"):
            is_regexp = True
            is_complex_regexp = True
            k = k[: -len("_complexregexp")]
        elif k.endswith("_like"):
            is_like = True
            k = k[: -len("_like")]
        if k in model.column_names:
            if isinstance(v, list):
                if is_between:
                    query = query.filter(getattr(model, k).between(*v))
                elif is_neq:
                    query = query.filter(getattr(model, k).not_in(v))
                else:
                    query = query.filter(getattr(model, k).in_(v))
            else:
                if isinstance(v, str):
                    v = v.strip()
                if is_neq:
                    query = query.filter(getattr(model, k) != v)
                elif is_le:
                    query = query.filter(getattr(model, k) <= v)
                elif is_ge:
                    query = query.filter(getattr(model, k) >= v)
                elif is_lt:
                    query = query.filter(getattr(model, k) < v)
                elif is_gt:
                    query = query.filter(getattr(model, k) > v)
                elif is_regexp:
                    value = v if is_complex_regexp else uniform_regexp_string(v)
                    query = query.filter(getattr(model, k).op("regexp")(value))
                elif is_like:
                    query = query.filter(getattr(model, k).like(v))
                else:
                    query = query.filter(getattr(model, k) == v)
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
