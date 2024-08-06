import os

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from jose import jwt, JWTError

from ui_api.routes.account import account_router
from ui_api.routes.auth import auth_router
from ui_api.routes.benchmark import benchmark_router
from ui_api.routes.company_facts import company_facts_router
from ui_api.routes.financials import financials_router
from database.async_database import db_connector

app = FastAPI()

origins = [
    "https://equityexplorer.io",
    "http://localhost",  # For local testing
    "http://localhost:3000"  # For local React dev server
]

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


class APIKeyJWTMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        excluded_paths = ["/login", "/register", "/authenticate"]

        if request.method == "OPTIONS" or request.url.path in excluded_paths:
            return await call_next(request)

        api_key = request.headers.get("X-API-Key")
        auth_header = request.headers.get("Authorization")

        if api_key:
            query = "SELECT id FROM users.users WHERE api_key = %s"
            result = db_connector.run_query(query, (api_key,))
            if result.empty:
                raise HTTPException(status_code=401, detail="Invalid API key")
            request.state.user_id = result.iloc[0]["id"]
            request.state.is_api_key = True

        elif auth_header:
            try:
                token = auth_header.replace("Bearer ", "")
                payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
                user_id: str = payload.get("sub")
                if user_id is None:
                    raise HTTPException(status_code=401, detail="Not authenticated")
                request.state.user_id = user_id
                request.state.is_api_key = False
            except JWTError:
                raise HTTPException(status_code=401, detail="Not authenticated")
        else:
            raise HTTPException(status_code=401, detail="Not authenticated")

        response = await call_next(request)
        return response


# To differentiate where requests are made from.
app.add_middleware(APIKeyJWTMiddleware)

app.include_router(auth_router)
app.include_router(company_facts_router)
app.include_router(benchmark_router)
app.include_router(financials_router, prefix='/financials')
app.include_router(account_router)
