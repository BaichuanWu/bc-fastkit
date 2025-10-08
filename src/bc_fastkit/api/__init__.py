from functools import wraps
from typing import List, Optional, Type

from fastapi import APIRouter, HTTPException
from fastapi.routing import APIRoute

from ..crud import CRUDBase
from ..schema import BaseSchema, CRUSchema
from .base import CRUDRequestHandler

COMMIT_SESSION_METHODS = {"PUT", "POST", "DELETE"}


def commit_session(f):
    @wraps(f)
    async def wrapper(*args, **kargs):
        response = await f(*args, **kargs)
        db = kargs.get("db")
        if db:
            try:
                db.commit()
            except Exception as e:
                db.rollback()
                raise HTTPException(status_code=400, detail=e.args[0])
        return response

    return wrapper


class CommitSessionRoute(APIRoute):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if set(kwargs["methods"]) & COMMIT_SESSION_METHODS:
            self.endpoint = commit_session(self.endpoint)


def create_commit_session_router(*args, **kwargs) -> "CRUDRouter":
    kwargs["route_class"] = CommitSessionRoute
    return CRUDRouter(*args, **kwargs)


class CRUDRouter(APIRouter):
    CRUD_METHODS = ("GET", "POST", "PUT", "DELETE")

    def crud(
        self,
        path: str,
        *,
        handler: CRUDBase,
        schema: CRUSchema,
        session_dep,
        methods: Optional[List[str]] = None,
        get_response_model: Optional[BaseSchema] = None,
        post_response_model: Optional[BaseSchema] = None,
        put_response_model: Optional[BaseSchema] = None,
        delete_response_model: Optional[BaseSchema] = None,
    ):
        methods = [m.upper() for m in methods] if methods else list(self.CRUD_METHODS)

        def decorator(cls: Type[CRUDRequestHandler]):
            request_handler = cls(handler, schema, session_dep)
            if "GET" in methods:
                self.add_api_route(
                    path=path,
                    endpoint=request_handler.get,
                    response_model=get_response_model or schema.QR,
                    methods=["GET"],
                )
            if "POST" in methods:
                self.add_api_route(
                    path=path,
                    endpoint=request_handler.post,
                    response_model=post_response_model or schema.R,
                    methods=["POST"],
                )
            if "PUT" in methods:
                self.add_api_route(
                    path=path,
                    endpoint=request_handler.put,
                    response_model=put_response_model or schema.R,
                    methods=["PUT"],
                )
            if "DELETE" in methods:
                self.add_api_route(
                    path=path,
                    endpoint=request_handler.delete,
                    response_model=delete_response_model or int,
                    methods=["DELETE"],
                )
            return request_handler

        return decorator
