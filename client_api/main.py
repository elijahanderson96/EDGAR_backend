from fastapi import FastAPI, APIRouter
from fastapi.middleware.cors import CORSMiddleware

from client_api.routes.financials import financials_router_client
from database.async_database import db_connector

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
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

api_router = APIRouter(prefix="/api")

# Include your routes under the /api prefix
api_router.include_router(financials_router_client, prefix='/financials')

app.include_router(api_router)
