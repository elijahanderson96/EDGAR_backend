import pandas as pd
import yfinance as yf
import asyncio
from edgar.symbols import symbols
from database.async_database import db_connector  # Assuming db_connector provides `run_query` method


def get_historical_prices(symbol, start_date, end_date):
    """
    Fetches historical end-of-day prices for a given stock symbol.
    """
    try:
        data = yf.download(symbol, start=start_date, end=end_date)
        data.columns = [col.lower().replace(" ", "_") for col in data.columns]
        data['symbol'] = symbol
        print(f"Fetched data for {symbol} from {start_date} to {end_date}")
        return data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None


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
        print(f"Error fetching metadata: {e}")
        return None, None


async def insert_data_to_db(df):
    """
    Merges the historical price data with symbol and date metadata,
    and performs a bulk insert into the historical_data table.
    """
    symbols_df, dates_df = await fetch_metadata()
    if symbols_df is None or dates_df is None:
        print("Skipping data insertion due to missing metadata.")
        return

    # Convert 'date' columns to datetime format
    df['date'] = pd.to_datetime(df.index)
    dates_df['date'] = pd.to_datetime(dates_df['date'])

    # Merge df with symbols_df and dates_df on symbol and date
    merged_df = df.merge(symbols_df, on='symbol', how='inner')
    merged_df = merged_df.merge(dates_df, on='date', how='inner')

    # Select relevant columns for insertion
    insert_df = merged_df[['symbol_id', 'date_id', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]

    # Retrieve rows not in the database
    rows_not_in_db = await db_connector.drop_existing_rows(insert_df[['symbol_id', 'date_id']],
                                                           "financials.historical_data", ["symbol_id", "date_id"])

    # Filter to keep only rows not in the database using `isin`
    if rows_not_in_db.empty:
        print("All rows already exist in the database. No new data to insert.")
        return

    insert_df = insert_df[
        (insert_df['symbol_id'].isin(rows_not_in_db['symbol_id']) &
         insert_df['date_id'].isin(rows_not_in_db['date_id']))
    ]

    # Prepare bulk insert values using the filtered insert_df
    values = [(
        row.symbol_id, row.date_id, row.open, row.high, row.low, row.close, row.adj_close, row.volume
    ) for row in insert_df.itertuples(index=False)]

    insert_query = """
    INSERT INTO financials.historical_data (symbol_id, date_id, open, high, low, close, adj_close, volume)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    ON CONFLICT (symbol_id, date_id) DO NOTHING;
    """

    try:
        # Use executemany to insert data in bulk
        async with db_connector.pool.acquire() as connection:
            await connection.executemany(insert_query, values)
        print(f"Inserted data for symbols in date range: {df['date'].min()} to {df['date'].max()}")
    except Exception as e:
        print(f"Error inserting data for symbol {df['symbol'].iloc[0]}: {e}")


async def process_symbol(symbol, start_date, end_date):
    """
    Fetches historical prices for a symbol, then inserts them into the database, handling each symbol sequentially.
    """
    try:
        df = get_historical_prices(symbol, start_date, end_date)
        if df is not None and not df.empty:
            await insert_data_to_db(df)

        # Add a delay to avoid hitting rate limits
        await asyncio.sleep(1)  # Adjust this delay as needed
    except Exception as e:
        print(f"Error processing symbol {symbol}: {e}")


async def main(start_date, end_date):
    await db_connector.initialize()
    symbols_list = symbols['symbol'].to_list()[0:5]  # Assuming symbols is a list of symbols to process

    for symbol in symbols_list:
        await process_symbol(symbol, start_date, end_date)  # Sequentially process each symbol

    await db_connector.close()


# Example usage
if __name__ == "__main__":
    start_date = "1900-01-01"
    end_date = "2025-01-01"
    asyncio.run(main(start_date, end_date))
