from fastapi import APIRouter, HTTPException
from fastapi_cache import FastAPICache
from fastapi_cache.backends.inmemory import InMemoryBackend
from fastapi_cache.decorator import cache
from database.async_database import db_connector
import logging

home_router = APIRouter()

logging.basicConfig(level=logging.INFO)


@home_router.on_event("startup")
async def startup():
    FastAPICache.init(InMemoryBackend(), prefix="fastapi-cache")


@home_router.get("/facts", tags=["Facts"])
@cache(expire=86400)  # Cache for 24 hours
async def get_latest_facts():
    """
    Fetches the latest data for specified fact names across all symbols.
    """

    try:
        query = "SELECT * FROM financials.latest_company_facts;"
        result = await db_connector.run_query(query)

        if result.empty:
            raise HTTPException(
                status_code=404,
                detail="No data found for the specified fact names."
            )

        # Pivot the DataFrame to have one row per symbol and each fact_name as a column
        pivoted_result = result.pivot_table(
            index='symbol',
            columns='fact_name',
            values='value',
            aggfunc='first'
        ).reset_index()
        # Replace NaN and infinite values with None
        pivoted_result = pivoted_result.replace([float('inf'), float('-inf')], None).where(pivoted_result.notnull(), None)
        return pivoted_result.to_dict(orient="records")

    except Exception as e:
        logging.error(f"Error fetching latest facts: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch latest facts.")


@home_router.get("/home", tags=["Home"])
async def get_daily_stock_changes():
    """
    Fetches daily stock price changes (price and percentage change) between the latest two available dates.
    """
    try:
        query = """
        WITH latest_date AS (
            SELECT 
                MAX(hd.date_id) AS current_date_id
            FROM 
                financials.historical_data hd
        ),
        previous_date AS (
            SELECT 
                MAX(hd.date_id) AS previous_date_id
            FROM 
                financials.historical_data hd
            WHERE 
                hd.date_id < (SELECT current_date_id FROM latest_date)
        ),
        current_data AS (
            SELECT 
                s.symbol,
                d.date AS current_date,
                hd.close AS current_close
            FROM 
                financials.historical_data hd
            JOIN 
                metadata.symbols s ON s.symbol_id = hd.symbol_id
            JOIN 
                metadata.dates d ON d.date_id = hd.date_id
            WHERE 
                hd.date_id = (SELECT current_date_id FROM latest_date)
        ),
        previous_data AS (
            SELECT 
                s.symbol,
                d.date AS previous_date,
                hd.close AS previous_close
            FROM 
                financials.historical_data hd
            JOIN 
                metadata.symbols s ON s.symbol_id = hd.symbol_id
            JOIN 
                metadata.dates d ON d.date_id = hd.date_id
            WHERE 
                hd.date_id = (SELECT previous_date_id FROM previous_date)
        )
        SELECT 
            c.symbol,
            c.current_date,
            p.previous_date,
            c.current_close,
            p.previous_close,
            (c.current_close - p.previous_close) AS price_change,
            CASE 
                WHEN p.previous_close = 0 THEN NULL
                ELSE ((c.current_close - p.previous_close) / p.previous_close) * 100
            END AS percentage_change
        FROM 
            current_data c
        JOIN 
            previous_data p ON c.symbol = p.symbol
        ORDER BY 
            c.symbol;
        """
        result = await db_connector.run_query(query)

        if result.empty:
            raise HTTPException(
                status_code=404,
                detail="No data found for daily stock changes."
            )

        return result.to_dict(orient="records")

    except Exception as e:
        logging.error(f"Error fetching daily stock changes: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch daily stock changes.")
