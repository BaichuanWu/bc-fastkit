from typing import Any, Generic

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from ....common.query import QUERY_TYPE_SIMPLE
from ....common.typing import D
from ...core.typing import ModelType


class AsyncCRUDHookMixin(Generic[ModelType]):
    async def complement_obj_in(self, db: AsyncSession, *, obj_in: D) -> D:
        if obj_in.get("cno"):
            obj_in["cno"] = obj_in["cno"].strip()
        none_keys = []
        for k, v in obj_in.items():
            if v is None:
                none_keys.append(k)
        for k in none_keys:
            obj_in.pop(k)
        return obj_in

    async def before_create(self, db: AsyncSession, *, obj_in: D) -> D:
        return await self.complement_obj_in(db, obj_in=obj_in)

    async def after_create(
        self, db: AsyncSession, *, obj_in: D, entity: ModelType
    ) -> ModelType:
        return entity

    async def before_update(self, db: AsyncSession, *, obj_in: D) -> D:
        return await self.complement_obj_in(db, obj_in=obj_in)

    async def after_update(
        self, db: AsyncSession, *, obj_in: D, entity: ModelType, prev: ModelType
    ) -> ModelType:
        return entity

    async def before_remove(self, db: AsyncSession, *, id: Any) -> Any:
        return id

    async def after_remove(self, db: AsyncSession, *, id: Any) -> Any:
        return id

    async def complete_query(
        self, db: AsyncSession, query: Select, typ=QUERY_TYPE_SIMPLE, **kwargs
    ) -> Select:
        return query

    async def complete_query_result(
        self, db: AsyncSession, data: Any, typ=QUERY_TYPE_SIMPLE, **kwargs
    ) -> Any:
        return data
