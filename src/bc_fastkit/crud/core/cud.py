from typing import Any, List, Type

from sqlalchemy import UniqueConstraint
from sqlalchemy.dialects.mysql import insert as dialect_insert
from sqlalchemy.orm import Session

from ...common.typing import D
from .typing import ModelType


def db_create(db: Session, *, obj_in: D, model: Type[ModelType]) -> ModelType:
    try:
        db_obj = model(
            **{k: v for k, v in obj_in.items() if k in model.creatable_column_names}
        )  # type: ignore
        db.add(db_obj)
        db.flush()
        db.refresh(db_obj)
    except Exception as e:
        db.rollback()
        raise e
    return db_obj


def db_create_or_update(db: Session, *, obj_in: D, model: Type[ModelType]):
    try:
        values = {k: v for k, v in obj_in.items() if k in model.creatable_column_names}

        update_values = {
            k: v for k, v in obj_in.items() if k in model.mutable_column_names
        }
        stmt = dialect_insert(model).values(**values)  # type: ignore
        stmt = stmt.on_duplicate_key_update(**update_values)
        db.execute(stmt)
    except Exception as e:
        db.rollback()
        raise e


def db_update(db: Session, *, obj_in: D, model: Type[ModelType]) -> ModelType | None:
    try:
        d: Any = {k: v for k, v in obj_in.items() if k in model.mutable_column_names}
        if d:
            db.query(model).filter(model.id == obj_in["id"]).update(d)
            db.flush()
    except Exception as e:
        db.rollback()
        raise e
    return db.query(model).get(obj_in["id"])


def db_multi_create(db: Session, *, obj_ins: List[D], model: Type[ModelType]):
    obj_in_datas = [
        {k: v for k, v in obj_in.items() if k in model.creatable_column_names}
        for obj_in in obj_ins
    ]
    try:
        db.bulk_insert_mappings(model, obj_in_datas)  # type: ignore
        db.flush()
    except Exception as e:
        db.rollback()
        raise e


def db_remove(db: Session, *, id: int, model: Type[ModelType]) -> int:
    if model.is_fake_delete:
        d: Any = {"is_deleted": 1}
        if model.unique_column_names:
            entity = db.query(model).get(id)
            delete_no = (
                db.query(model)
                .filter(
                    getattr(model, model.unique_column_names[0]).op("regexp")(
                        f"^{getattr(entity, model.unique_column_names[0])}{model.FAKE_DELETE_UK_SUFFIX}"
                    )
                )
                .count()
                + 1
            )
            for unique_column in model.unique_column_names:
                d[unique_column] = (
                    f"{getattr(entity, unique_column)}{model.FAKE_DELETE_UK_SUFFIX}{delete_no:03d}"
                )
        db.query(model).filter(model.id == id).update(d)
    elif model.is_real_delete:
        obj = db.query(model).get(id)
        db.delete(obj)
    else:
        db.rollback()
        raise ValueError(f"模型{model}未配置删除方式")
    db.flush()
    return id


def db_multi_replacement_update(
    db: Session,
    old_entities: List[ModelType],
    new_objs: List[D],
    model: Type[ModelType],
):
    unique_keys = [
        u
        for u in model.__table__.constraints  # type: ignore
        if isinstance(u, UniqueConstraint)  # type: ignore
    ]
    if unique_keys:
        column_names = [c.name for c in unique_keys[0].columns]
        pairs = []
        for old in old_entities:
            new = [
                n
                for n in new_objs
                if all([n[c] == getattr(old, c) for c in column_names])
            ]
            if new:
                new[0]["id"] = old.id
                db_update(db, obj_in=new[0], model=model)
                pairs.append(
                    tuple([new[0][column_name] for column_name in column_names])
                )
        old_entities = [
            old
            for old in old_entities
            if tuple([getattr(old, column_name) for column_name in column_names])
            not in pairs
        ]
        new_objs = [
            new
            for new in new_objs
            if tuple([new[column_name] for column_name in column_names]) not in pairs
        ]
    remove_entities = []
    create_objs = []
    if len(old_entities) > len(new_objs):
        remove_entities = old_entities[len(new_objs) : len(old_entities)]
        update_entities = old_entities[0 : len(new_objs)]
    elif len(old_entities) <= len(new_objs):
        create_objs = new_objs[len(old_entities) : len(new_objs)]
        update_entities = old_entities
    if model.is_real_delete:
        db.query(model).filter(model.id.in_([m.id for m in remove_entities])).delete()
    else:
        db.query(model).filter(model.id.in_([m.id for m in remove_entities])).update(
            {"is_deleted": 1}
        )
    db_multi_create(db, obj_ins=create_objs, model=model)
    for idx, m in enumerate(update_entities):
        new_objs[idx]["id"] = m.id
        db_update(db, obj_in=new_objs[idx], model=model)
