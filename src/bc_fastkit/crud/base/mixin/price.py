# from decimal import Decimal
# from typing import List, Any

# from sqlalchemy.orm import Session

# from app.common import TAX_TYP_TAX_CONTAIN, QUERY_TYPE_OVERALL
# from app.common.bill import BillTypeEnum
# from app.model.base import BaseBillModel, BaseModel
# from app.core.typing import D

# from .hook import CRUDHookMixin


# class BillPriceMixIn(CRUDHookMixin):

#     PRICE_ATTRS = ["currency_id", "items", "discount", "tax_typ"]
#     PRICE_ITEM_ATTRS = ["untaxed_price", "tax_rate"]

#     def complement_obj_in(self, db: Session, *, obj_in: D) -> D:
#         obj_in = super().complement_obj_in(db, obj_in=obj_in)
#         return self.update_obj_in_meta_price(db, obj_in=obj_in)

#     def update_obj_in_meta_price(self, db: Session, obj_in: D) -> D:
#         from app.crud import currency_handler

#         if not all(i in obj_in for i in self.PRICE_ATTRS):
#             return obj_in
#         items = obj_in["items"]
#         currency = currency_handler.get(db, id=obj_in["currency_id"])
#         obj_in["currency_rate"] = currency.mean_rate
#         for item in items:
#             item["currency_rate"] = obj_in["currency_rate"]
#             item["currency_id"] = obj_in["currency_id"]
#             item["discount"] = obj_in["discount"]
#             item["tax_typ"] = obj_in["tax_typ"]
#             tax_rate = Decimal((1 + item["tax_rate"] / 100))
#             if "price" not in item and all(i in item for i in self.PRICE_ITEM_ATTRS):
#                 item["taxed_price"] = item["untaxed_price"] * tax_rate
#             elif obj_in["tax_typ"] == TAX_TYP_TAX_CONTAIN:
#                 item["untaxed_price"] = item["price"] / tax_rate
#                 item["taxed_price"] = item["price"]
#             else:
#                 item["untaxed_price"] = item["price"]
#                 item["taxed_price"] = item["price"] * tax_rate
#         if not obj_in.get("untaxed_amount"):
#             obj_in["untaxed_amount"] = obj_in["discount"] * sum(
#                 [i["untaxed_price"] * i["quantity"] for i in items]
#             )
#         if not obj_in.get("taxed_amount"):
#             obj_in["taxed_amount"] = obj_in["discount"] * sum(
#                 [i["taxed_price"] * i["quantity"] for i in items]
#             )
#         return obj_in

#     def complete_query_result(self, db: Session, data: Any, typ=..., **kwargs) -> Any:
#         data = super().complete_query_result(db, data, typ, **kwargs)
#         if typ == QUERY_TYPE_OVERALL:
#             if kwargs.get("to_bill_typ") in [
#                 BillTypeEnum.finance_pay.value,
#                 BillTypeEnum.finance_receipt.value,
#                 BillTypeEnum.finance_pre_pay_requisition.value,
#                 BillTypeEnum.finance_pre_receipt_requisition.value,
#             ]:

#                 return prepare_shifted_for_pay_or_receipt(
#                     db,
#                     data,
#                     kwargs["to_bill_typ"],
#                     kwargs.get("q", {}).get("show_shifted", False),
#                 )
#         return data


# def prepare_shifted_for_pay_or_receipt(
#     db: Session,
#     data: List[BaseBillModel],
#     to_bill_typ: BillTypeEnum,
#     show_shifted: bool,
# ) -> List[BaseBillModel]:
#     from app.crud import get_bill_handler

#     if not data:
#         return data
#     to_handler = get_bill_handler(to_bill_typ)
#     to_items = to_handler.search_item(
#         db, q={"source_id": [d.id for d in data], "source_typ": data[0].bill_typ}
#     )
#     amount_dict = {}
#     for to_item in to_items:
#         if to_item.source_id not in amount_dict:
#             amount_dict[to_item.source_id] = to_item.amount
#         else:
#             amount_dict[to_item.source_id] += to_item.amount

#     for d in data:
#         d.shifted_amount = amount_dict.get(d.id, 0)
#     if not show_shifted:
#         return [d for d in data if d.amount > d.shifted_amount]
#     return data


# def complement_price_from_source(
#     db: Session, entities: List[BaseModel], mapping: D = None
# ):
#     from app.crud import get_bill_handler

#     mapping = mapping or {
#         "discount_price": "discount_price",
#         "tax_rate": "tax_rate",
#         "tax_typ": "tax_typ",
#         "currency_id": "currency_id",
#     }
#     source_typ_dict = {}
#     for e in entities:
#         if getattr(e, "items", []):
#             source_typ_dict.setdefault(e.items[0].source_typ, []).append(e)
#     for source_typ, d in source_typ_dict.items():
#         handler = get_bill_handler(source_typ)
#         source_item_ids = []
#         for d in entities:
#             source_item_ids.extend([i.source_item_id for i in d.items])
#         source_item_dict = handler.get_items_dict(db, ids=source_item_ids)
#         if source_typ in (BillTypeEnum.purchase_in.value, BillTypeEnum.sale_out.value):
#             order_handler = get_bill_handler(
#                 BillTypeEnum.purchase_order.value
#                 if source_typ == BillTypeEnum.purchase_in.value
#                 else BillTypeEnum.sale_order.value
#             )
#             order_item_dict = order_handler.get_items_dict(
#                 db, ids=[v.source_item_id for v in source_item_dict.values()]
#             )
#             for v in source_item_dict.values():
#                 order_item = order_item_dict.get(v.source_item_id)
#                 v.discount_price = order_item and order_item.discount_price or 0
#         for d in entities:
#             for i in d.items:
#                 order_item = source_item_dict.get(i.source_item_id)
#                 if order_item:
#                     for attr, source_attr in mapping.items():
#                         setattr(i, attr, getattr(order_item, source_attr, 0))
#     return entities
