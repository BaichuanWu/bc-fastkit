import inspect
from typing import Any, TypeVar

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from ..common.query import QUERY_TYPE_OVERALL, CommonQueryParams
from ..crud import AsyncCRUDBase, CRUDBase
from ..schema import BaseSchema, CRUSchema, QueryResponseSchema

HandlerType = TypeVar("HandlerType", bound=CRUDBase)
SessionType = TypeVar("SessionType", bound=Session | AsyncSession)


def inner_json_encoder(data: BaseSchema, **kwargs):
    return data.model_dump(exclude_none=True, **kwargs)


async def maybe_await(obj):
    if inspect.isawaitable(obj):
        return await obj
    return obj


class CRUDRequestHandler:
    def __init__(
        self,
        handler: CRUDBase | AsyncCRUDBase,
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
            return await self.respond_get(db, common)

        return fn

    @property
    def post(self):
        async def fn(
            data: self.schema.C,  # type: ignore
            db: self.session_dep,  # type: ignore
        ) -> Any:
            return await self.respond_post(db, obj_in=data)

        return fn

    @property
    def put(self):
        async def fn(
            data: self.schema.U,  # type: ignore
            db: self.session_dep,  # type: ignore
        ) -> Any:
            return await self.respond_put(db, obj_in=data)

        return fn

    @property
    def delete(self):
        async def fn(
            id: int,
            db: self.session_dep,  # type: ignore
        ) -> Any:
            return await self.respond_delete(db, id=id)

        return fn

    async def respond_get(
        self,
        db: Session | AsyncSession,
        common: CommonQueryParams = CommonQueryParams(),
    ):
        res = self.handler.search_limit(
            db,
            q=common.q,
            typ=common.query_typ,
            skip=common.skip,
            limit=common.limit,
        )
        data, total = await maybe_await(res)
        return QueryResponseSchema(
            data_source=data,
            total=total,
            query=common.to_dict(),
            update_time=common.update_time,
        )

    async def respond_post(
        self,
        db: Session | AsyncSession,
        *,
        obj_in,
    ):
        entity = await maybe_await(
            self.handler.create(
                db,
                obj_in={
                    **inner_json_encoder(obj_in),
                },
            )
        )
        if isinstance(entity, list):
            res = self.handler.search(
                db, q={"id": [e.id for e in entity]}, typ=QUERY_TYPE_OVERALL
            )
        else:
            res = self.handler.search_one(
                db, q={"id": entity.id}, typ=QUERY_TYPE_OVERALL
            )
        return await maybe_await(res)

    async def respond_put(
        self,
        db: Session | AsyncSession,
        *,
        obj_in,
    ):
        entity = await maybe_await(
            self.handler.update(
                db,
                obj_in=inner_json_encoder(obj_in),
            )
        )
        res = self.handler.search_one(db, q={"id": entity.id}, typ=QUERY_TYPE_OVERALL)
        return await maybe_await(res)

    async def respond_delete(self, db: Session | AsyncSession, *, id: int):
        return await maybe_await(self.handler.remove(db, id=id))
