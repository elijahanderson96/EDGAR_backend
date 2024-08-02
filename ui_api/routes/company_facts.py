# main.py
from datetime import datetime

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
        # Get the current date
        current_date = datetime.now().date()

        # Get the start of the current quarter
        current_quarter_start = current_date.replace(day=1, month=((current_date.month - 3) // 3) * 3 + 1)
        print(current_quarter_start)

        # Fetch the table for non-discontinued series
        table_query = f"""
        SELECT *
        FROM company_facts.{symbol}
        WHERE fact_key IN (
            SELECT DISTINCT fact_key
            FROM company_facts.{symbol}
            WHERE filed_date >= '{current_quarter_start}'
        )
        """
        table_results = db_connector.run_query(table_query, return_df=True)

        # Convert the table results to a list of dictionaries
        company_facts_data = table_results.to_dict(orient='records')

        # Generate the metadata from the table_results
        fact_keys = table_results['fact_key'].unique().tolist()
        descriptions = table_results.groupby('fact_key')['description'].first().tolist()
        company_facts_metadata = dict(zip(fact_keys, descriptions))

        return {
            'metadata': company_facts_metadata,
            'data': company_facts_data
        }

    except Exception as e:
        print(f"Error fetching company facts: {str(e)}")
        return {"error": "Failed to fetch company facts"}