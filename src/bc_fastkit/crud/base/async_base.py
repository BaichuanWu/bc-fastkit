# type: ignore
from typing import Any, AsyncGenerator, Dict, List, Optional, Tuple, Type

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from ...common.query import QUERY_TYPE_OVERALL, QUERY_TYPE_SIMPLE
from ...common.typing import D
from ..core.async_cud import (
    db_async_create,
    db_async_create_or_update,
    db_async_multi_create,
    db_async_remove,
    db_async_update,
)
from ..core.async_query import async_sql_filter, async_sql_page_filter
from ..core.query import uniform_regexp_string
from ..core.typing import ModelType
from .mixin.async_hook import AsyncCRUDHookMixin


class AsyncCRUDBase(
    AsyncCRUDHookMixin[ModelType],
):
    def __init__(self, model: Type[ModelType]):
        self.model = model

    async def get(self, db: AsyncSession, id: Any) -> Optional[ModelType]:
        stmt = async_sql_filter(
            q={"id": id}, query=select(self.model), model=self.model
        )
        result = await db.execute(stmt)
        return result.scalars().first()

    async def gets(self, db: AsyncSession, ids: List[int] = None) -> List[ModelType]:
        stmt = async_sql_filter(
            q={"id": ids} if ids is not None else {},
            query=select(self.model),
            model=self.model,
        )
        result = await db.execute(stmt)
        return list(result.scalars().all())

    async def gets_dict(
        self, db: AsyncSession, ids: List[int] = None
    ) -> Dict[int, ModelType]:
        entities = await self.gets(db, ids)
        return {e.id: e for e in entities}

    async def lock(self, db: AsyncSession, id: Any) -> Optional[ModelType]:
        stmt = async_sql_filter(
            q={"id": id}, query=select(self.model), model=self.model
        )
        stmt = stmt.with_for_update()
        result = await db.execute(stmt)
        return result.scalars().first()

    async def get_by_cno(self, db: AsyncSession, cno: Any) -> Optional[ModelType]:
        if hasattr(self.model, "cno"):
            stmt = async_sql_filter(
                q={"cno": cno}, query=select(self.model), model=self.model
            )
            result = await db.execute(stmt)
            return result.scalars().first()

    def query_sk(self, query: Select, sk: str) -> Select:
        return query

    async def complete_query(
        self, db: AsyncSession, query: Select, typ=..., **kwargs
    ) -> Select:
        if typ == QUERY_TYPE_OVERALL:
            sk = kwargs.get("q", {}).get("sk")
            if sk:
                query = self.query_sk(query, uniform_regexp_string(sk))
        return await super().complete_query(db, query, typ, **kwargs)

    async def query(
        self, db: AsyncSession, q: D, typ=QUERY_TYPE_SIMPLE, **kwargs
    ) -> Select:
        stmt = await self.complete_query(db, select(self.model), typ, q=q, **kwargs)
        return async_sql_filter(q, stmt, self.model)

    def parse_order_by(self, order_by: List[Any]) -> List[Any]:
        if not order_by:
            return order_by
        parsed = []
        for item in order_by:
            if isinstance(item, str):
                desc = item.startswith("-")
                clean_item = (
                    item[1:] if item.startswith("-") or item.startswith("+") else item
                )
                if hasattr(self.model, clean_item):
                    col = getattr(self.model, clean_item)
                    parsed.append(col.desc() if desc else col.asc())
            else:
                parsed.append(item)
        return parsed

    async def search_limit(
        self,
        db: AsyncSession,
        q: D,
        order_by: List[Any] = None,
        typ=QUERY_TYPE_SIMPLE,
        skip=0,
        limit=9999,
        **kwargs,
    ) -> Tuple[List[ModelType], int]:
        stmt = await self.query(db, q, typ)
        data, total = await async_sql_page_filter(
            db=db,
            q={},
            query=stmt,
            model=self.model,
            skip=skip,
            limit=limit,
            order_by=(
                self.parse_order_by(order_by)
                if order_by
                else self.get_query_order(typ, q)
            ),
        )
        return (
            await self.complete_query_result(db=db, data=data, typ=typ, q=q, **kwargs),
            total,
        )

    async def search(
        self, db: AsyncSession, q: D, order_by: List[Any] = None, typ=QUERY_TYPE_SIMPLE
    ) -> List[ModelType]:
        stmt = await self.query(db, q, typ)
        parsed_order_by = (
            self.parse_order_by(order_by) if order_by else self.get_query_order(typ, q)
        )
        stmt = stmt.order_by(*parsed_order_by)
        result = await db.execute(stmt)
        data = list(result.scalars().all())
        return await self.complete_query_result(db=db, data=data, typ=typ)

    async def search_iter(
        self, db: AsyncSession, q: D, batch_size: int = 500, typ=QUERY_TYPE_SIMPLE
    ) -> AsyncGenerator[List[ModelType], None]:
        last_id = 0
        while True:
            stmt = await self.query(db, q, typ)
            stmt = (
                stmt.filter(self.model.id > last_id)
                .order_by(self.model.id.asc())
                .limit(batch_size)
            )
            result = await db.execute(stmt)
            data = list(result.scalars().all())

            if not data:
                break

            completed_data = await self.complete_query_result(db=db, data=data, typ=typ)
            for item in completed_data:
                yield item

            last_id = data[-1].id
            if len(data) < batch_size:
                break

    async def search_one(
        self, db: AsyncSession, q: D, order_by: List[Any] = None, typ=QUERY_TYPE_SIMPLE
    ) -> Optional[ModelType]:
        stmt = await self.query(db, q, typ)
        parsed_order_by = (
            self.parse_order_by(order_by) if order_by else self.get_query_order(typ, q)
        )
        stmt = stmt.order_by(*parsed_order_by)
        result = await db.execute(stmt.limit(1))
        data = result.scalars().first()
        if data:
            completed = await self.complete_query_result(db=db, data=[data], typ=typ)
            return completed[0]
        return None

    async def search_total(self, db: AsyncSession, q: D) -> int:
        stmt = await self.query(db, q)
        count_stmt = select(func.count()).select_from(stmt.subquery())
        result = await db.execute(count_stmt)
        return result.scalar() or 0

    def get_query_order(self, typ, q):
        return [self.model.id.desc()]

    async def create(self, db: AsyncSession, *, obj_in: D) -> ModelType:
        obj_in = await self.before_create(db, obj_in=obj_in)
        if obj_in is None:
            return
        elif isinstance(obj_in, list):
            res = []
            for obj in obj_in:
                entity = await db_async_create(db, obj_in=obj, model=self.model)
                res.append(await self.after_create(db, obj_in=obj, entity=entity))
            return res
        else:
            entity = await db_async_create(db, obj_in=obj_in, model=self.model)
            return await self.after_create(db, obj_in=obj_in, entity=entity)

    async def raw_create(self, db: AsyncSession, *, obj_in: D) -> ModelType:
        return await db_async_create(db, obj_in=obj_in, model=self.model)

    async def raw_update(self, db: AsyncSession, *, obj_in: D) -> ModelType:
        return await db_async_update(db, obj_in=obj_in, model=self.model)

    async def raw_remove(self, db: AsyncSession, *, id: Any) -> Any:
        return await db_async_remove(db, id=id, model=self.model)

    async def create_on_duplicate_update(
        self, db: AsyncSession, *, obj_in: D
    ) -> ModelType:
        obj_in = await self.before_create(db, obj_in=obj_in)
        return await self.raw_create_or_update(db, obj_in=obj_in)

    async def raw_create_or_update(self, db: AsyncSession, *, obj_in: D) -> ModelType:
        return await db_async_create_or_update(db, obj_in=obj_in, model=self.model)

    async def update(self, db: AsyncSession, *, obj_in: D) -> ModelType:
        obj_in = await self.before_update(db, obj_in=obj_in)
        if not obj_in:
            return
        entity = await self.get(db, id=obj_in["id"])
        if not entity:
            return
        # Use a copy helper for prev?
        # In current CRUDBase it uses entity.copy(). Assuming BaseModel has copy()
        # SQLAlchemy objects might need manual copy for 'prev' if they are tracked.
        # For simplicity, we fetch it or use the one we have.
        # But wait, db_async_update might modify the session object.
        # Let's try to get a snapshot.
        prev = entity.to_dict() if hasattr(entity, "to_dict") else None

        updated_entity = await db_async_update(db, obj_in=obj_in, model=self.model)
        return await self.after_update(
            db, obj_in=obj_in, entity=updated_entity, prev=prev
        )

    async def multi_create(self, db: AsyncSession, *, obj_ins: List[D]):
        processed_objs = []
        for obj_in in obj_ins:
            processed_objs.append(await self.before_create(db, obj_in=obj_in))
        return await db_async_multi_create(
            db,
            obj_ins=processed_objs,
            model=self.model,
        )

    async def remove(self, db: AsyncSession, *, id: Any) -> Any:
        await self.before_remove(db, id=id)
        removed_id = await db_async_remove(db, id=id, model=self.model)
        return await self.after_remove(db, id=removed_id)

    async def get_update_changes(self, db: AsyncSession, obj_in: D, raw=False):
        entity = await self.get(db, obj_in["id"])
        from .base import get_entity_update_from_obj_in

        return get_entity_update_from_obj_in(obj_in, entity, raw=raw)
