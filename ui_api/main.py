import os
import logging
import sys

from fastapi import FastAPI
from fastapi.openapi.docs import get_swagger_ui_html
from jose import jwt

from ui_api.routes.account import account_router
from ui_api.routes.auth import auth_router
from ui_api.routes.data_catalogue import stocks_router
from ui_api.routes.financials import financials_router
from ui_api.routes.home import home_router

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


# remove these lines to show the health check if you want to ensure the load balancer is
# properly communicating with the server.
logging.getLogger("uvicorn.access").addFilter(DocsFilter())

logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
            "format": "%(asctime)s - %(levelname)s - %(message)s",
        },
    },
    "handlers": {
        "default": {
            "formatter": "default",
            "class": "logging.StreamHandler",
            "stream": sys.stdout,
        },
    },
    "loggers": {
        "uvicorn": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.error": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
        "uvicorn.access": {
            "handlers": ["default"],
            "level": "INFO",
            "propagate": False,
        },
    },
}

logging.config.dictConfig(logging_config)


class APIKeyJWTMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # auth routes are excluded so users can obtain access/refresh tokens.
        excluded_paths = ["/login", "/register", "/authenticate", "/docs", "/refresh", "/authenticate/{auth_token}",
                          "/openapi.json"]

        # Skip logging for the /docs endpoint
        if request.url.path == "/docs":
            return await call_next(request)

        if request.method == "OPTIONS" or any(request.url.path.startswith(path) for path in excluded_paths):
            return await call_next(request)

        #api_key = request.headers.get("X-API-Key")
        api_key = '52b205d5-93e0-4d68-a202-8b081638de4e'
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
    "http://localhost:3000",  # For local React dev server
    "http://localhost:8000"  # Add this if Swagger UI is served from this port

]

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


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html(req: Request):
    root_path = req.scope.get("root_path", "").rstrip("/")
    openapi_url = root_path + app.openapi_url
    return get_swagger_ui_html(
        openapi_url=openapi_url,
        title="API",
    )


SECRET_KEY = os.getenv("JWT_SECRET_KEY")
ALGORITHM = "HS256"

app.include_router(auth_router)
app.include_router(financials_router)
app.include_router(account_router)
app.include_router(metadata_router)
app.include_router(stocks_router, prefix='/stocks')
app.include_router(home_router)
