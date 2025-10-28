from typing import Any, Generic

from sqlalchemy.orm import Query, Session

from ....common.query import QUERY_TYPE_SIMPLE
from ....common.typing import D
from ...core.typing import ModelType


class CRUDHookMixin(Generic[ModelType]):
    def complement_obj_in(self, db: Session, *, obj_in: D) -> D:
        if obj_in.get("cno"):
            obj_in["cno"] = obj_in["cno"].strip()
        none_keys = []
        for k, v in obj_in.items():
            if v is None:
                none_keys.append(k)
        for k in none_keys:
            obj_in.pop(k)
        return obj_in

    def before_create(self, db: Session, *, obj_in: D) -> D:
        return self.complement_obj_in(db, obj_in=obj_in)

    def after_create(self, db: Session, *, obj_in: D, entity: ModelType) -> ModelType:
        return entity

    def before_update(self, db: Session, *, obj_in: D) -> D:
        return self.complement_obj_in(db, obj_in=obj_in)

    def after_update(
        self, db: Session, *, obj_in: D, entity: ModelType, prev: ModelType
    ) -> ModelType:
        return entity

    def before_remove(self, db: Session, *, id: int) -> int:
        return id

    def after_remove(self, db: Session, *, id: int) -> int:
        return id

    def complete_query(
        self, db: Session, query: Query, typ=QUERY_TYPE_SIMPLE, **kwargs
    ) -> Query:
        return query

    def complete_query_result(
        self, db: Session, data: Any, typ=QUERY_TYPE_SIMPLE, **kwargs
    ) -> Any:
        return data
