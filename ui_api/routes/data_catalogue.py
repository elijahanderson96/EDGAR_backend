from fastapi import APIRouter, HTTPException, Query
from database.async_database import db_connector
import logging

stocks_router = APIRouter()

logging.basicConfig(level=logging.INFO)


@stocks_router.get("/symbols", tags=["Symbols"])
async def get_symbols():
    """
    Returns a list of stock symbols for the search bar component.
    """
    try:
        symbols = await db_connector.run_query('SELECT symbol FROM metadata.symbols')
        return symbols['symbol'].tolist()
    except Exception as e:
        logging.error(f"Error fetching symbols: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch symbols")


@stocks_router.get("/facts", tags=["Financial Facts"])
async def get_financial_facts(
        symbol: str = Query(..., description="Stock symbol to filter by (e.g., 'AAPL')"),
        fact_name: str = Query(..., description="Fact name to filter by (e.g., 'EarningsPerShareBasic')")
):
    """
    Fetches financial facts for a given stock symbol and fact name.
    """
    try:
        query = """
        SELECT
            cf.id,
            s.symbol AS company_symbol,
            cf.fact_name,
            ds_start.date AS start_date,
            ds_end.date AS end_date,
            ds_filed.date AS filed_date,
            cf.fiscal_year,
            cf.fiscal_period,
            cf.form,
            cf.value,
            cf.accn
        FROM
            financials.company_facts cf
        JOIN
            metadata.symbols s ON cf.symbol_id = s.symbol_id
        LEFT JOIN
            metadata.dates ds_start ON cf.start_date_id = ds_start.date_id
        LEFT JOIN
            metadata.dates ds_end ON cf.end_date_id = ds_end.date_id
        LEFT JOIN
            metadata.dates ds_filed ON cf.filed_date_id = ds_filed.date_id
        WHERE
            s.symbol = $1
            AND cf.fact_name = $2;
        """
        params = (symbol, fact_name)
        result = await db_connector.run_query(query, params)

        if result.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No data found for symbol '{symbol}' and fact name '{fact_name}'."
            )

        return result.to_dict(orient="records")

    except Exception as e:
        logging.error(f"Error fetching financial facts: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch financial facts.")
