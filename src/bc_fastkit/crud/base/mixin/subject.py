# from typing import Iterable, Union

# from fastapi import HTTPException, status
# from sqlalchemy.orm import Session

# from app.common.bill import BillEventEnum
# from app.common.event import CRUDEventEnum, Enum

# from app.model import BaseBillModel
# from app.core.pattern import Subject, Observer
# from app.core.typing import D
# from .hook import CRUDHookMixin
# from ...core import ModelType, BillModelType


# class CUDSubjectMixin(Subject, CRUDHookMixin[ModelType]):
#     DEFAULT_EVENTS = [
#         CRUDEventEnum.after_create,
#         CRUDEventEnum.after_update,
#         CRUDEventEnum.before_remove,
#     ]

#     @property
#     def observers(self):
#         if not hasattr(self, "_observers"):
#             self._observers = {}
#         return self._observers

#     def attach(
#         self,
#         observer: Observer,
#         events: Union[Enum, Iterable[Enum]] = None,
#     ) -> None:
#         events = self.DEFAULT_EVENTS if events is None else events
#         if isinstance(events, Enum):
#             events = [events]
#         for event in events:
#             self.observers.setdefault(event, []).append(observer)

#     def detach(self, observer: Observer, *args, **kwargs) -> None:
#         return

#     def notify(self, event: CRUDEventEnum, *args, **kwargs) -> None:
#         for observer in self.observers.get(event, []):
#             observer.update(self, event, *args, **kwargs)

#     def after_create(self, db: Session, *, obj_in: D, entity: ModelType):
#         self.notify(CRUDEventEnum.after_create, db, obj_in=obj_in, entity=entity)
#         return super().after_create(db, obj_in=obj_in, entity=entity)

#     def after_update(
#         self, db: Session, *, obj_in: D, entity: ModelType, prev: ModelType
#     ):
#         self.notify(
#             CRUDEventEnum.after_update, db, obj_in=obj_in, entity=entity, prev=prev
#         )
#         return super().after_update(db, obj_in=obj_in, entity=entity, prev=prev)

#     def before_remove(self, db: Session, *, id: int) -> int:
#         self.notify(CRUDEventEnum.before_remove, db, id=id)
#         return super().before_remove(db, id=id)


# class BillSubjectMixin(CUDSubjectMixin[BillModelType]):
#     DEFAULT_EVENTS = (
#         BillEventEnum.commit,
#         BillEventEnum.audit_fail,
#         BillEventEnum.audit_pass,
#         BillEventEnum.modify_pass,
#         BillEventEnum.close,
#         BillEventEnum.update_item_hold_quantity,
#         BillEventEnum.cancel,
#     )

#     def on_commit(self, db: Session, bill: BillModelType):
#         self.notify(BillEventEnum.commit, db, bill)

#     def on_audit_pass(self, db: Session, bill: BillModelType):
#         self.notify(BillEventEnum.audit_pass, db, bill)

#     def on_audit_fail(self, db: Session, bill: BillModelType):
#         self.notify(BillEventEnum.audit_fail, db, bill)

#     def on_modify_pass(self, db: Session, bill: BillModelType):
#         self.notify(BillEventEnum.modify_pass, db, bill)

#     def on_close(self, db: Session, bill: BillModelType):
#         self.notify(BillEventEnum.close, db, bill)

#     def on_cancel(self, db: Session, bill: BillModelType):
#         self.notify(BillEventEnum.cancel, db, bill)

#     def on_state_change(
#         self, db: Session, bill: BillModelType, state: int, prev_state: int
#     ):
#         # 超级管理员可以控制，权限限制转移
#         # if prev_state >= BaseBillModel.STATE_CLOSED:
#         #     raise HTTPException(
#         #         status_code=status.HTTP_400_BAD_REQUEST, detail="当前状态不支持改变"
#         #     )
#         if state == BaseBillModel.STATE_COMMIT:
#             self.on_commit(db, bill=bill)
#         elif state == BaseBillModel.STATE_AUDITED:
#             if prev_state == BaseBillModel.STATE_MODIFY_COMMIT:
#                 self.on_modify_pass(db, bill=bill)
#             elif prev_state < BaseBillModel.STATE_COMMIT:
#                 self.on_commit(db, bill=bill)
#                 self.on_audit_pass(db, bill=bill)
#             else:
#                 self.on_audit_pass(db, bill=bill)
#         elif state == BaseBillModel.STATE_REJECTED:
#             self.on_audit_fail(db, bill=bill)
#         elif state == BaseBillModel.STATE_CLOSED:
#             self.on_close(db, bill=bill)
#         elif state == BaseBillModel.STATE_CANCELLED:
#             if not self.model.CANCELABLE:
#                 raise HTTPException(
#                     status_code=status.HTTP_400_BAD_REQUEST, detail="内部调用异常：此类单据不支持取消"
#                 )
#             elif prev_state < BaseBillModel.STATE_COMMIT:
#                 raise HTTPException(
#                     status_code=status.HTTP_400_BAD_REQUEST, detail="未提交单据不支持取消"
#                 )
#             else:
#                 self.on_cancel(db, bill)

#     def before_remove(self, db: Session, *, id: int):
#         rs = super().before_remove(db, id=id)
#         self.notify(BillEventEnum.delete, db, id)
#         return rs

#     def after_update(
#         self, db: Session, *, obj_in: D, entity: BillModelType, prev: BillModelType
#     ):
#         entity = super().after_update(db, obj_in=obj_in, entity=entity, prev=prev)
#         if obj_in.get("state") is not None and obj_in["state"] != prev.state:
#             self.on_state_change(
#                 db, bill=entity, state=entity.state, prev_state=prev.state
#             )
#         return entity
