# from typing import List, Optional, Type, Dict

# from sqlalchemy.orm.session import Session

# from app.common import QUERY_TYPE_OVERALL
# from app.core.typing import D
# from app.utils.search import sql_filter
# from app.model import Relation

# from .hook import CRUDHookMixin, ModelType
# from ...core.cud import db_multi_replacement_update


# class CRUDRelationMixin(CRUDHookMixin[ModelType]):
#     @property
#     def relation_dict(self) -> Dict[str, Relation]:
#         return self.model.relation_dict

#     def create_or_update_all_relations(
#         self, db: Session, *, obj_in: D, entity: ModelType
#     ):
#         for k, m in self.relation_dict.items():
#             if isinstance(obj_in.get(k, None), list):
#                 self.create_or_update_relations(
#                     db,
#                     cond={m.column_name: entity.id},
#                     mappings=obj_in[k],
#                     model=m.relation_model,
#                 )

#     def after_create(self, db: Session, *, obj_in: D, entity: ModelType):
#         entity = super().after_create(db, obj_in=obj_in, entity=entity)
#         self.create_or_update_all_relations(db, obj_in=obj_in, entity=entity)
#         return entity

#     def after_update(
#         self, db: Session, *, obj_in: D, entity: ModelType, prev: ModelType
#     ):
#         entity = super().after_update(db, obj_in=obj_in, entity=entity, prev=prev)
#         self.create_or_update_all_relations(db, obj_in=obj_in, entity=entity)
#         return entity

#     def complete_query_result(self, db: Session, data, typ=..., **kwargs):
#         data = super().complete_query_result(db, data, typ, **kwargs)
#         if typ == QUERY_TYPE_OVERALL:
#             ids = [d.id for d in data]
#             for r in self.relation_dict.values():
#                 if r.is_one_to_many:
#                     target = self.get_mapping_dict(db, r.attr_name, ids)
#                     for d in data:
#                         setattr(d, r.attr_name, target.get(d.id, []))
#         return data

#     def create_or_update_relations(
#         cls,
#         db: Session,
#         cond: D,
#         mappings: Optional[D],
#         model: Type[ModelType],
#         old_relations: List[ModelType] = None,
#     ):
#         # TODO confirm id
#         if mappings is None:
#             return
#         for m in mappings:
#             for k, v in cond.items():
#                 m[k] = v
#         if old_relations is None:
#             old_relations = sql_filter(cond, db.query(model), model=model).all()
#         db_multi_replacement_update(
#             db, old_entities=old_relations, new_objs=mappings, model=model
#         )

#     def get_mapping_dict(
#         self,
#         db: Session,
#         relation_key: str,
#         entity_ids: List[int],
#     ) -> D:
#         relation = self.relation_dict[relation_key]
#         model = (
#             relation.peer_model if relation.is_one_to_many else relation.mapping_model
#         )
#         rs = sql_filter(
#             {relation.column_name: entity_ids} if entity_ids else {},
#             db.query(model),
#             model=model,
#         ).all()
#         d = {}
#         for r in rs:
#             d.setdefault(getattr(r, relation.column_name), []).append(r)
#         return d

#     def get_mapping_peer_dict(
#         self,
#         db: Session,
#         relation_key: str,
#         entity_ids: List[int] = None,
#     ) -> D:
#         relation = self.relation_dict[relation_key]
#         query = db.query(relation.mapping_model, relation.peer_model).join(
#             relation.peer_model,
#             relation.peer_model.id
#             == getattr(relation.mapping_model, relation.peer_column_name),
#         )
#         rs = sql_filter(
#             {relation.column_name: entity_ids} if entity_ids else {},
#             query,
#             model=relation.mapping_model,
#         ).all()
#         d = {}
#         for m, t in rs:
#             d.setdefault(getattr(m, relation.column_name), []).append((m, t))
#         return d
