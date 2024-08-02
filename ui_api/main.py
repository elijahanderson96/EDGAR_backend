from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from ui_api.routes.auth import auth_router
from ui_api.routes.benchmark import benchmark_router
from ui_api.routes.company_facts import company_facts_router
from ui_api.routes.financials import financials_router
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


app.include_router(auth_router)
app.include_router(company_facts_router)
app.include_router(benchmark_router)
app.include_router(financials_router, prefix='/financials')
