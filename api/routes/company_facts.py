# main.py
from fastapi import APIRouter
from database.database import db_connector

company_facts_router = APIRouter()


@company_facts_router.get("/company_facts/search")
def search(term: str):
    query = """
        SELECT cik, symbol, title
        FROM company_facts.cik_mapping
        WHERE LOWER(symbol) LIKE %s OR LOWER(title) LIKE %s
    """
    params = (f"%{term.lower()}%", f"%{term.lower()}%")
    results = db_connector.run_query(query, params=params)

    if not results.empty:
        results_dict = results.set_index('cik').to_dict(orient='index')
        return results_dict
    else:
        return {"message": "No results found"}