import re
from typing import Any, Dict, List

from _strptime import TimeRE

DATE_STR_FORMAT = "%Y%m%d"
ZERO_TIME_STR = "1900-01-01"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"
DATE_FORMAT = "%Y-%m-%d"

PHONE_RE = re.compile(r"^1\d{10}$")
PRODUCT_MARK_RE = re.compile(r"^\d+(?:\s*\*\s*\d+){0,2}$")
FLOAT_INT_RE = re.compile(r"^\d+(\.\d+)?$")

D = Dict[str, Any]
L = List[Any]
# class D(dict):

#     def __init__(self, value=None):
#         if value is None:
#             pass
#         elif isinstance(value, dict):
#             for key in value:
#                 self.__setitem__(key, value[key])
#         else:
#             raise TypeError('expected dict')
#     @classmethod
#     def set_list(cls, values):
#         for idx, v in enumerate(values):
#             if isinstance(v, list):
#                 values[idx] = cls.set_list(v)
#             elif isinstance(v, dict) and not isinstance(v, D):
#                 values[idx] = D(v)
#         return values

#     def __setitem__(self, key, value):
#         if isinstance(value, dict) and not isinstance(value, D):
#             value = D(value)
#         elif isinstance(value, list):
#             value = self.set_list(values=value)
#         super(D, self).__setitem__(key, value)

#     def __getattr__(self, key):
#         return self[key]

#     __setattr__ = __setitem__


def is_float_or_int(s: str):
    return FLOAT_INT_RE.match(s.strip())


time_re = TimeRE()
datetime_re = time_re.compile(DATETIME_FORMAT)
date_re = time_re.compile(DATE_FORMAT)
