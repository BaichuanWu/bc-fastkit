from typing import Any, TypeVar

from fastapi import Depends
from sqlalchemy.orm import Session

from ..common import QUERY_TYPE_OVERALL
from ..common.query import CommonQueryParams
from ..crud import CRUDBase
from ..schema import BaseSchema, CRUSchema, QueryResponseSchema

HandlerType = TypeVar("HandlerType", bound=CRUDBase)
SessionType = TypeVar("SessionType", bound=Session)


def inner_json_encoder(data: BaseSchema, **kwargs):
    return data.model_dump(exclude_none=True, **kwargs)


class CRUDRequestHandler:
    def __init__(
        self,
        handler: CRUDBase,
        schema: CRUSchema,
        session_dep,
    ) -> None:
        self.handler = handler
        self.schema = schema
        self.session_dep = session_dep

    @property
    def model(self):
        return self.handler.model

    @property
    def get(self):
        async def fn(
            db: self.session_dep,  # type: ignore
            common=Depends(CommonQueryParams),
        ) -> Any:
            return self.respond_get(db, common)

        return fn

    @property
    def post(self):
        async def fn(
            data: self.schema.C,  # type: ignore
            db: self.session_dep,  # type: ignore
        ) -> Any:
            return self.respond_post(db, obj_in=data)

        return fn

    @property
    def put(self):
        async def fn(
            data: self.schema.U,  # type: ignore
            db: self.session_dep,  # type: ignore
        ) -> Any:
            return self.respond_put(db, obj_in=data)

        return fn

    @property
    def delete(self):
        async def fn(
            id: int,
            db: self.session_dep,  # type: ignore
        ) -> Any:
            return self.respond_delete(db, id=id)

        return fn

    def respond_get(
        self,
        db: Session,
        common: CommonQueryParams = CommonQueryParams(),
    ):
        data, total = self.handler.search_limit(
            db,
            q=common.q,
            typ=common.query_typ,
            skip=common.skip,
            limit=common.limit,
        )
        return QueryResponseSchema(
            data_source=data,
            total=total,
            query=common.to_dict(),
            update_time=common.update_time,
        )

    def respond_post(
        self,
        db: Session,
        *,
        obj_in,
    ):
        entity = self.handler.create(
            db,
            obj_in={
                **inner_json_encoder(obj_in),
            },
        )
        if isinstance(entity, list):
            return self.handler.search(
                db, q={"id": [e.id for e in entity]}, typ=QUERY_TYPE_OVERALL
            )
        else:
            return self.handler.search_one(
                db, q={"id": entity.id}, typ=QUERY_TYPE_OVERALL
            )

    def respond_put(
        self,
        db: Session,
        *,
        obj_in,
    ):
        entity = self.handler.update(
            db,
            obj_in=inner_json_encoder(obj_in),
        )
        return self.handler.search_one(db, q={"id": entity.id}, typ=QUERY_TYPE_OVERALL)

    def respond_delete(self, db: Session, *, id: int):
        return self.handler.remove(db, id=id)
