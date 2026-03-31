from typing import Any, List, Type

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.mysql import insert as dialect_insert
from sqlalchemy.ext.asyncio import AsyncSession

from ...common.typing import D
from .typing import ModelType


async def db_async_create(
    db: AsyncSession, *, obj_in: D, model: Type[ModelType]
) -> ModelType:
    try:
        db_obj = model(
            **{k: v for k, v in obj_in.items() if k in model.creatable_column_names}
        )  # type: ignore
        db.add(db_obj)
        await db.flush()
        await db.refresh(db_obj)
    except Exception as e:
        await db.rollback()
        raise e
    return db_obj


async def db_async_create_or_update(
    db: AsyncSession, *, obj_in: D, model: Type[ModelType]
):
    try:
        values = {k: v for k, v in obj_in.items() if k in model.creatable_column_names}

        update_values = {
            k: v for k, v in obj_in.items() if k in model.mutable_column_names
        }
        stmt = dialect_insert(model).values(**values)  # type: ignore
        stmt = stmt.on_duplicate_key_update(**update_values)
        await db.execute(stmt)
    except Exception as e:
        await db.rollback()
        raise e


async def db_async_update(
    db: AsyncSession, *, obj_in: D, model: Type[ModelType]
) -> ModelType | None:
    try:
        d: Any = {k: v for k, v in obj_in.items() if k in model.mutable_column_names}
        if d:
            stmt = update(model).where(model.id == obj_in["id"]).values(d)
            await db.execute(stmt)
            await db.flush()
    except Exception as e:
        await db.rollback()
        raise e

    # Re-fetch the object
    result = await db.execute(select(model).where(model.id == obj_in["id"]))
    return result.scalars().first()


async def db_async_multi_create(
    db: AsyncSession, *, obj_ins: List[D], model: Type[ModelType]
):
    obj_in_datas = [
        {k: v for k, v in obj_in.items() if k in model.creatable_column_names}
        for obj_in in obj_ins
    ]
    try:
        # Use simple insert for multiple values if driver supports it
        # bulk_insert_mappings is not directly available on AsyncSession in the same way
        # db.execute(insert(model), obj_in_datas) is preferred for SQLAlchemy 2.0 async
        from sqlalchemy import insert

        await db.execute(insert(model), obj_in_datas)
        await db.flush()
    except Exception as e:
        await db.rollback()
        raise e


async def db_async_remove(db: AsyncSession, *, id: Any, model: Type[ModelType]) -> Any:
    if model.is_fake_delete:
        d: Any = {"is_deleted": 1}
        if model.unique_column_names:
            # Fetch entity to get unique column values
            result = await db.execute(select(model).where(model.id == id))
            entity = result.scalars().first()
            if not entity:
                return id

            # Count suffixes for fake delete
            first_unique_col = getattr(model, model.unique_column_names[0])
            pattern = f"^{getattr(entity, model.unique_column_names[0])}{model.FAKE_DELETE_UK_SUFFIX}"
            count_stmt = select(model).where(first_unique_col.op("regexp")(pattern))
            count_result = await db.execute(count_stmt)
            delete_no = len(count_result.scalars().all()) + 1

            for unique_column in model.unique_column_names:
                val = getattr(entity, unique_column)
                d[unique_column] = f"{val}{model.FAKE_DELETE_UK_SUFFIX}{delete_no:03d}"

        await db.execute(update(model).where(model.id == id).values(d))
    elif model.is_real_delete:
        await db.execute(delete(model).where(model.id == id))
    else:
        await db.rollback()
        raise ValueError(f"模型{model}未配置删除方式")

    await db.flush()
    return id
