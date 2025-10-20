# type: ignore
from datetime import date
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Type

from sqlalchemy.orm import Query, Session

from ...common.query import QUERY_TYPE_OVERALL, QUERY_TYPE_SIMPLE
from ...common.typing import DATE_FORMAT, DATETIME_FORMAT, D, date_re, datetime_re
from ...model import BaseModel
from ..core.cud import (
    ModelType,
    db_create,
    db_create_or_update,
    db_multi_create,
    db_remove,
    db_update,
)
from ..core.query import sql_filter, uniform_regexp_string

# from .mixin.subject import CUDSubjectMixin
from .mixin.hook import CRUDHookMixin


class CRUDBase(
    # CUDSubjectMixin[ModelType],
    CRUDHookMixin[ModelType],
):
    def __init__(self, model: Type[ModelType]):
        self.model = model

    def get(self, db: Session, id: Any) -> Optional[ModelType]:
        return sql_filter(
            q={"id": id}, query=db.query(self.model), model=self.model
        ).first()

    def gets(self, db: Session, ids: List[int] = None) -> List[ModelType]:
        return sql_filter(
            q={"id": ids} if ids is not None else {},
            query=db.query(self.model),
            model=self.model,
        ).all()

    def gets_dict(self, db: Session, ids: List[int] = None) -> Dict[int, ModelType]:
        entities = self.gets(db, ids)
        return {e.id: e for e in entities}

    def lock(self, db: Session, id: Any) -> Optional[ModelType]:
        return (
            sql_filter(q={"id": id}, query=db.query(self.model), model=self.model)
            .populate_existing()
            .with_for_update()
            .one()
        )

    def get_by_cno(self, db: Session, cno: Any) -> Optional[ModelType]:
        if hasattr(self.model, "cno"):
            return sql_filter(
                q={"cno": cno}, query=db.query(self.model), model=self.model
            ).first()

    def query_sk(self, query: Query, sk: str) -> Query:
        return query

    def complete_query(self, db: Session, query: Query, typ=..., **kwargs) -> Query:
        if typ == QUERY_TYPE_OVERALL:
            sk = kwargs.get("q", {}).get("sk")
            if sk:
                query = self.query_sk(query, uniform_regexp_string(sk))
        return super().complete_query(db, query, typ, **kwargs)

    def query(self, db: Session, q: D, typ=QUERY_TYPE_SIMPLE, **kwargs) -> Query:
        query = self.complete_query(db, db.query(self.model), typ, q=q, **kwargs)
        return sql_filter(q, query, self.model)

    def search_limit(
        self,
        db: Session,
        q: D,
        order_by: List[Any] = None,
        typ=QUERY_TYPE_SIMPLE,
        skip=0,
        limit=9999,
        **kwargs,
    ) -> Tuple[ModelType, int]:
        query = self.query(db, q, typ)
        data = (
            query.order_by(*(order_by or self.get_query_order(typ, q)))
            .offset(skip)
            .limit(limit)
            .all()
        )
        return (
            self.complete_query_result(db=db, data=data, typ=typ, q=q, **kwargs),
            query.count(),
        )

    def search(
        self, db: Session, q: D, order_by: List[Any] = None, typ=QUERY_TYPE_SIMPLE
    ) -> List[ModelType]:
        data = (
            self.query(db, q, typ)
            .order_by(*(order_by or self.get_query_order(typ, q)))
            .all()
        )
        return self.complete_query_result(db=db, data=data, typ=typ)

    def search_one(
        self, db: Session, q: D, order_by: List[Any] = None, typ=QUERY_TYPE_SIMPLE
    ) -> ModelType:
        data = (
            self.query(db, q, typ)
            .order_by(*(order_by or self.get_query_order(typ, q)))
            .first()
        )
        if data:
            return self.complete_query_result(db=db, data=[data], typ=typ)[0]
        else:
            return

    def search_total(self, db: Session, q: D) -> int:
        return self.query(db, q).count()

    def get_query_order(self, typ, q):
        return [self.model.id.desc()]

    def create(self, db: Session, *, obj_in: D) -> ModelType:
        obj_in = self.before_create(db, obj_in=obj_in)
        if obj_in is None:
            return
        elif isinstance(obj_in, list):
            return [
                self.after_create(
                    db,
                    obj_in=obj,
                    entity=db_create(db, obj_in=obj, model=self.model),
                )
                for obj in obj_in
            ]
        else:
            entity = db_create(db, obj_in=obj_in, model=self.model)
            return self.after_create(db, obj_in=obj_in, entity=entity)

    def raw_create(self, db: Session, *, obj_in: D) -> ModelType:
        return db_create(db, obj_in=obj_in, model=self.model)

    def raw_update(self, db: Session, *, obj_in: D) -> ModelType:
        return db_update(db, obj_in=obj_in, model=self.model)

    def raw_remove(self, db: Session, *, id: int) -> ModelType:
        return db_remove(db, id=id, model=self.model)

    def raw_create_or_update(self, db: Session, *, obj_in: D) -> ModelType:
        return db_create_or_update(db, obj_in=obj_in, model=self.model)

    # TODO 确认before 只负责补全 obj_in(包括额外操作生成id之类), after 只负责side effect
    def update(self, db: Session, *, obj_in: D) -> ModelType:
        obj_in = self.before_update(db, obj_in=obj_in)
        if not obj_in:
            return
        entity = self.get(db, id=obj_in["id"])
        if not entity:
            return
        prev = entity.copy()
        entity = db_update(db, obj_in=obj_in, model=self.model)
        return self.after_update(db, obj_in=obj_in, entity=entity, prev=prev)

    def multi_create(self, db: Session, *, obj_ins: List[D]):
        """
        无after create 影响
        """
        return db_multi_create(
            db,
            obj_ins=[self.before_create(db, obj_in=obj_in) for obj_in in obj_ins],
            model=self.model,
        )

    def remove(self, db: Session, *, id: int) -> int:
        self.before_remove(db, id=id)
        return self.after_remove(db, id=db_remove(db, id=id, model=self.model))

    def get_update_changes(self, db: Session, obj_in: D, raw=False):
        entity = self.get(db, obj_in["id"])
        return get_entity_update_from_obj_in(obj_in, entity, raw=raw)


def check_value_update(attr_name: str, obj_in: D, entity: BaseModel):
    if attr_name not in obj_in or attr_name in entity.immutable_column_names:
        return False
    elif attr_name not in entity.column_names:
        return True
    elif isinstance(getattr(entity, attr_name), Decimal):
        if isinstance(obj_in[attr_name], float):
            new_v = Decimal(str(obj_in[attr_name]))
        else:
            new_v = obj_in[attr_name]
        return round(new_v, 8) - round(getattr(entity, attr_name), 8)
    elif isinstance(getattr(entity, attr_name), date) and isinstance(
        obj_in[attr_name], str
    ):
        format = ""
        if datetime_re.match(obj_in[attr_name]):
            format = DATETIME_FORMAT
        elif date_re.match(obj_in[attr_name]):
            format = DATE_FORMAT
        return (
            not format
            or getattr(entity, attr_name).strftime(format) != obj_in[attr_name]
        )
    elif obj_in[attr_name] != getattr(entity, attr_name):
        return True
    return False


def get_entity_update_from_obj_in(obj_in: D, entity: BaseModel, raw=False):
    rs = {}
    for attr_name in obj_in.keys():
        if check_value_update(attr_name, obj_in=obj_in, entity=entity):
            rs[attr_name] = obj_in[attr_name]
    if raw:
        return {k: v for k, v in rs.items() if k in entity.mutable_column_names}
    else:
        return rs
