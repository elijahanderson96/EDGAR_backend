import os
import logging

from fastapi import FastAPI
from jose import jwt

from ui_api.routes.account import account_router
from ui_api.routes.auth import auth_router
from ui_api.routes.financials import financials_router
from database.async_database import db_connector

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.cors import CORSMiddleware

from fastapi import Request, status
from fastapi.responses import JSONResponse
from jose import JWTError, ExpiredSignatureError

from ui_api.routes.metadata import metadata_router

app = FastAPI()


class DocsFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        # Filter out logs that contain /docs
        return "/docs" not in record.getMessage()


logging.getLogger("uvicorn.access").addFilter(DocsFilter())


class APIKeyJWTMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        excluded_paths = ["/login", "/register", "/authenticate", "/docs"]

        # Skip logging for the /docs endpoint
        if request.url.path == "/docs":
            return await call_next(request)

        if request.method == "OPTIONS" or request.url.path in excluded_paths:
            return await call_next(request)

        api_key = request.headers.get("X-API-Key")
        auth_header = request.headers.get("Authorization")

        if api_key:
            query = "SELECT id FROM users.users WHERE api_key = $1"
            result = await db_connector.run_query(query, (api_key,))
            if result.empty:
                return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"detail": "Invalid API key"})
            request.state.user_id = result.iloc[0]["id"]
            request.state.is_api_key = True

        elif auth_header:
            try:
                token = auth_header.replace("Bearer ", "")
                payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
                user_id: str = payload.get("sub")
                if user_id is None:
                    return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED,
                                        content={"detail": "Not authenticated"})
                request.state.user_id = user_id
                request.state.is_api_key = False
            except ExpiredSignatureError:
                return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"detail": "Token expired"})
            except JWTError:
                return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"detail": "Not authenticated"})
        else:
            return JSONResponse(status_code=status.HTTP_401_UNAUTHORIZED, content={"detail": "Not authenticated"})

        response = await call_next(request)
        return response


origins = [
    "https://equityexplorer.io",
    "http://localhost",  # For local testing
    "http://localhost:3000"  # For local React dev server
]

# To differentiate where requests are made from.
app.add_middleware(APIKeyJWTMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    await db_connector.initialize()


@app.on_event("shutdown")
async def shutdown():
    await db_connector.close()


SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ALGORITHM = "HS256"

app.include_router(auth_router)
app.include_router(financials_router, prefix='/financials')
app.include_router(account_router)
app.include_router(metadata_router)
