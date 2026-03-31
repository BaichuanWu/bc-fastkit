import json
from datetime import datetime
from typing import List, Optional

from .uitls import deep_hump2underline, deep_underline2hump, hump2underline

QUERY_TYPE_SIMPLE = 0
QUERY_TYPE_OVERALL = 1
QUERY_TYPE_WORKBENCH = 2
QUERY_TYPE_AGGREGATION = 4
QUERY_TYPE_PRINT = 5


class CommonQueryParams:
    QUERY_TYP_MASK = 7

    def __init__(
        self,
        q: Optional[str] = None,
        skip: int = 0,
        limit: int = 20,
        orderBy: Optional[str] = None,
        typ: int = QUERY_TYPE_SIMPLE,
    ):
        self.q = (
            q
            if isinstance(q, dict)
            else (
                {
                    k: v
                    for k, v in deep_hump2underline(json.loads(q)).items()
                    if v != "" and not (isinstance(v, list) and len(v) == 0)
                }
                if q
                else {}
            )
        )
        self.skip = skip
        self.limit = limit
        self.order_by = self._parse_order_by(orderBy)
        self.typ = typ
        self.update_time = datetime.now()

    @staticmethod
    def _parse_order_by(orderBy: Optional[str]) -> Optional[List[str]]:
        """解析前端传入的 orderBy 逗号分隔字符串, 如 '-sharpe,fitness', 字段名驼峰转蛇形"""
        if not orderBy:
            return None
        result = []
        for item in orderBy.split(","):
            item = item.strip()
            if not item:
                continue
            if item.startswith("-") or item.startswith("+"):
                result.append(item[0] + hump2underline(item[1:]))
            else:
                result.append(hump2underline(item))
        return result or None

    @property
    def query_typ(self):
        return self.typ

    def to_dict(self):
        return {
            "q": deep_underline2hump(self.q),
            "skip": self.skip,
            "limit": self.limit,
            "typ": self.typ,
        }
