from fastapi import APIRouter, HTTPException
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
