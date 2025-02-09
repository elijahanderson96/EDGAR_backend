import logging
import time
from asyncio import sleep
import argparse
import yfinance as yf
import asyncio
from edgar.symbols import symbols
from database.async_database import db_connector
from utils.type_matcher import cast_dataframe_to_table_schema

import pandas as pd
import io

# ✅ Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ✅ Global Counter for Total Rows Inserted
total_rows_inserted = 0


async def insert_dataframe_to_db(df: pd.DataFrame):
    """
    Insert only new rows from df into financials.historical_data, avoiding duplicates.
    """
    global total_rows_inserted  # Track total rows inserted

    existing_data = await db_connector.run_query(
        'SELECT symbol_id, date_id FROM financials.historical_data WHERE symbol_id = ANY($1)',
        params=[df['symbol_id'].tolist()]  # ✅ asyncpg-compatible list format
    )

    logging.info(f"Existing Data: {len(existing_data)} rows fetched.")
    logging.info(f"New Data: {df.shape[0]} rows before filtering.")

    if not existing_data.empty:
        existing_df = pd.DataFrame(existing_data, columns=['symbol_id', 'date_id'])

        df = df.merge(existing_df, how="left", indicator=True).query("_merge == 'left_only'").drop(columns=["_merge"])

    logging.info(f"Filtered Data: {df.shape[0]} rows remain for insertion.")

    if 'id' in df.columns:
        df.drop(labels=['id'], inplace=True, axis='columns')

    df = await cast_dataframe_to_table_schema(df, 'financials', 'historical_data')

    if df.empty:
        logging.info("✅ No new data to insert.")
        return

    output = io.BytesIO()
    df.to_csv(output, sep='\t', index=False, header=False, na_rep='NULL')
    output.seek(0)

    async with db_connector.pool.acquire() as connection:
        try:
            await connection.copy_to_table(
                'historical_data',
                schema_name='financials',
                source=output,
                format='csv',
                delimiter='\t',
                columns=df.columns.tolist()
            )
            total_rows_inserted += df.shape[0]
            logging.info(f"✅ Inserted {df.shape[0]} new records into the database.")
        except Exception as e:
            logging.error(f"🚨 Error inserting into database")


def get_historical_prices(symbols, start_date, end_date):
    """
    Fetches historical end-of-day prices for multiple stock symbols.
    """
    try:

        data = yf.download(symbols, start=start_date, end=end_date, group_by='ticker')
        all_data = []

        for symbol in data.columns.levels[0]:  # Fix deprecated `.levels[0]` warning
            symbol_data = data[symbol].copy()
            symbol_data.columns = [col.lower().replace(" ", "_") for col in symbol_data.columns]
            symbol_data.dropna(subset=['open', 'high', 'low', 'close'], how='all', inplace=True)
            symbol_data['symbol'] = symbol
            all_data.append(symbol_data)
            logging.info(f"✅ Fetched data for {symbol} from {start_date} to {end_date}")

        return pd.concat(all_data) if all_data else pd.DataFrame()

    except Exception as e:
        logging.info(f"🚨 Error fetching data for symbols: {e}")
        return pd.DataFrame()


async def fetch_metadata():
    """
    Fetches the symbol_id and date_id mappings from the metadata tables and returns them as DataFrames.
    """
    try:
        symbols_query = "SELECT symbol_id, symbol FROM metadata.symbols"
        symbols_df = await db_connector.run_query(symbols_query)

        dates_query = "SELECT date_id, date FROM metadata.dates"
        dates_df = await db_connector.run_query(dates_query)

        return symbols_df, dates_df

    except Exception as e:
        logging.info(f"🚨 Error fetching metadata: {e}")
        return None, None


async def insert_data_to_db(df):
    """
    Merges the historical price data with symbol and date metadata,
    and performs a bulk insert into the historical_data table using copy_to_table.
    """
    symbols_df, dates_df = await fetch_metadata()
    if symbols_df is None or dates_df is None:
        logging.info("🚨 Skipping data insertion due to missing metadata.")
        return

    df = df.copy()  # Ensure it's a copy to avoid SettingWithCopyWarning
    df['date'] = pd.to_datetime(df.index)

    dates_df = dates_df.copy()
    dates_df['date'] = pd.to_datetime(dates_df['date'])

    # Merge with metadata
    merged_df = df.merge(symbols_df, on='symbol', how='inner')
    merged_df = merged_df.merge(dates_df, on='date', how='inner')

    insert_df = merged_df[['symbol_id', 'date_id', 'open', 'high', 'low', 'close', 'volume']].copy()

    if 'volume' in insert_df.columns:
        insert_df.loc[:, 'volume'] = insert_df['volume'].fillna(0).astype(float).astype(int)

    try:
        await insert_dataframe_to_db(insert_df)
    except Exception as e:
        logging.info(f"🚨 Error inserting data into database: {e}")


async def process_symbols(symbols, start_date, end_date):
    """
    Fetches historical prices for multiple symbols, aggregates the data, and inserts it into the database.
    """
    try:
        df = get_historical_prices(symbols, start_date, end_date)
        if df is not None and not df.empty:
            chunk_size = 3000000
            for start in range(0, len(df), chunk_size):
                chunk_df = df.iloc[start:start + chunk_size]
                await insert_data_to_db(chunk_df)
    except Exception as e:
        logging.info(f"🚨 Error processing symbols: {e}")


async def main(start_date, end_date):
    """
    Main function to process all stock symbols in batches and insert historical data.
    """
    global total_rows_inserted
    start_time = time.time()  # Track execution start time

    await db_connector.initialize()

    symbols_list = symbols['symbol'].to_list()
    batch_size = 10

    for i in range(0, len(symbols_list), batch_size):
        await sleep(3)
        batch_symbols = symbols_list[i:i + batch_size]
        await process_symbols(batch_symbols, start_date, end_date)

    await db_connector.close()

    total_time = time.time() - start_time
    logging.info(f"🚀 Total Rows Inserted: {total_rows_inserted}")
    logging.info(f"⏳ Total Execution Time: {total_time:.2f} seconds")


def parse_arguments():
    """
    Parses command-line arguments for start and end dates.
    """
    parser = argparse.ArgumentParser(description="Fetch historical financial data.")

    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="Start date in YYYY-MM-DD format (e.g., 2000-01-01)"
    )

    parser.add_argument(
        "--end-date",
        type=str,
        required=False,
        help="End date in YYYY-MM-DD format (e.g., 2025-01-01)"
    )

    return parser.parse_args()


if __name__ == "__main__":
    # Usage below:
    # python fetch_data.py --start-date 2000-01-01 --end-date 2025-01-01
    args = parse_arguments()
    asyncio.run(main(args.start_date, args.end_date))
