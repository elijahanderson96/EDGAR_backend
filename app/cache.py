import logging
from datetime import date
from typing import Dict, Optional

import pandas as pd

from database.async_database import db_connector

logger = logging.getLogger(__name__)

# In-memory caches
symbols_cache: Dict[str, int] = {} # {symbol: symbol_id}
dates_cache: Dict[date, int] = {}   # {date_obj: date_id}
symbol_id_to_symbol: Dict[int, str] = {} # {symbol_id: symbol} - Reverse mapping might be useful
date_id_to_date: Dict[int, date] = {}     # {date_id: date_obj} - Reverse mapping

async def load_symbols_cache():
    """Loads symbol data from metadata.symbols into the cache."""
    global symbols_cache, symbol_id_to_symbol
    logger.info("Loading symbols cache...")
    query = "SELECT symbol_id, symbol FROM metadata.symbols"
    try:
        symbols_df = await db_connector.run_query(query, return_df=True)
        if symbols_df is not None and not symbols_df.empty:
            symbols_cache = pd.Series(symbols_df.symbol_id.values, index=symbols_df.symbol).to_dict()
            symbol_id_to_symbol = pd.Series(symbols_df.symbol.values, index=symbols_df.symbol_id).to_dict()
            logger.info(f"Symbols cache loaded successfully with {len(symbols_cache)} entries.")
        else:
            logger.warning("No symbols found in metadata.symbols to load into cache.")
    except Exception as e:
        logger.error(f"Failed to load symbols cache: {e}", exc_info=True)
        # Depending on requirements, might want to raise error or proceed with empty cache

async def load_dates_cache():
    """Loads date data from metadata.dates into the cache."""
    global dates_cache, date_id_to_date
    logger.info("Loading dates cache...")
    query = "SELECT date_id, date FROM metadata.dates"
    try:
        dates_df = await db_connector.run_query(query, return_df=True)
        if dates_df is not None and not dates_df.empty:
            # Ensure 'date' column is datetime.date objects
            dates_df['date'] = pd.to_datetime(dates_df['date']).dt.date
            dates_cache = pd.Series(dates_df.date_id.values, index=dates_df.date).to_dict()
            date_id_to_date = pd.Series(dates_df.date.values, index=dates_df.date_id).to_dict()
            logger.info(f"Dates cache loaded successfully with {len(dates_cache)} entries.")
        else:
            logger.warning("No dates found in metadata.dates to load into cache.")
    except Exception as e:
        logger.error(f"Failed to load dates cache: {e}", exc_info=True)

def get_symbol_id(symbol: str) -> Optional[int]:
    """Retrieves symbol_id from cache for a given symbol."""
    return symbols_cache.get(symbol.upper()) # Assuming symbols are stored uppercase

def get_date_id(date_str: str) -> Optional[int]:
    """Retrieves date_id from cache for a given date string (YYYY-MM-DD)."""
    try:
        date_obj = date.fromisoformat(date_str)
        return dates_cache.get(date_obj)
    except (ValueError, TypeError):
        logger.warning(f"Invalid date format provided: {date_str}. Use YYYY-MM-DD.")
        return None

def get_symbol_from_id(symbol_id: int) -> Optional[str]:
    """Retrieves symbol from cache for a given symbol_id."""
    return symbol_id_to_symbol.get(symbol_id)

def get_date_from_id(date_id: int) -> Optional[date]:
    """Retrieves date object from cache for a given date_id."""
    return date_id_to_date.get(date_id)
