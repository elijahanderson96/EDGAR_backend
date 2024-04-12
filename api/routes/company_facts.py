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


@company_facts_router.get("/company_facts/facts")
def get_company_facts(symbol: str):
    try:
        # Fetch the distinct fact_keys and their descriptions
        distinct_query = f"SELECT DISTINCT fact_key, description FROM company_facts.{symbol}"
        distinct_results = db_connector.run_query(distinct_query, return_df=True)

        # Fetch the entire table
        table_query = f"SELECT * FROM company_facts.{symbol}"
        table_results = db_connector.run_query(table_query, return_df=True)

        # Convert the distinct results to a dictionary
        fact_keys = distinct_results['fact_key'].tolist()
        descriptions = distinct_results['description'].tolist()
        company_facts_metadata = dict(zip(fact_keys, descriptions))

        # Convert the table results to a list of dictionaries
        company_facts_data = table_results.to_dict(orient='records')

        return {
            'metadata': company_facts_metadata,
            'data': company_facts_data
        }
    except Exception as e:
        print(f"Error fetching company facts: {str(e)}")
        return {"error": "Failed to fetch company facts"}