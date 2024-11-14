import yfinance as yf
import pandas as pd
import asyncio
from edgar.symbols import symbols
from database.async_database import db_connector  # Assuming db_connector provides `db` with `run_query` method


def get_historical_prices(symbol, start_date, end_date):
    """
    Fetches historical end-of-day prices for a given stock symbol.

    Parameters:
        symbol (str): The stock ticker symbol (e.g., "AAPL" for Apple Inc.).
        start_date (str): The start date for the data in 'YYYY-MM-DD' format.
        end_date (str): The end date for the data in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: A DataFrame containing historical end-of-day prices.
    """
    try:
        # Fetch the historical data for the symbol
        data = yf.download(symbol, start=start_date, end=end_date)

        # Rename columns to lowercase and remove spaces
        data.columns = [col.lower().replace(" ", "_") for col in data.columns]

        # Add the symbol as a column
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

    Parameters:
        df (pd.DataFrame): The DataFrame containing historical price data with 'symbol' and 'date' columns.
    """
    symbols_df, dates_df = await fetch_metadata()
    if symbols_df is None or dates_df is None:
        print("Skipping data insertion due to missing metadata.")
        return

    # Convert 'date' columns to datetime format
    df['date'] = pd.to_datetime(df.index)
    dates_df['date'] = pd.to_datetime(dates_df['date'])

    # Merge df with symbols_df and dates_df on symbol and date
    merged_df = df.merge(symbols_df, left_on='symbol', right_on='symbol', how='inner')
    merged_df = merged_df.merge(dates_df, left_on='date', right_on='date', how='inner')

    # Select relevant columns
    insert_df = merged_df[['symbol_id', 'date_id', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]

    # Prepare bulk insert values
    values = ", ".join(
        f"({row.symbol_id}, {row.date_id}, {row.open}, {row.high}, {row.low}, {row.close}, {row.adj_close}, {row.volume})"
        for _, row in insert_df.iterrows()
    )

    # Bulk insert query
    query = f"""
    INSERT INTO financials.historical_data (symbol_id, date_id, open, high, low, close, adj_close, volume)
    VALUES {values}
    ON CONFLICT (symbol_id, date_id) DO NOTHING;
    """

    try:
        await db_connector.run_query(query, return_df=False)
        print(f"Inserted data for symbols in date range: {df['date'].min()} to {df['date'].max()}")
    except Exception as e:
        print(f"Error inserting data for symbol {df['symbol'].iloc[0]}: {e}")


async def process_symbol(symbol, start_date, end_date):
    """
    Fetches historical prices for a symbol, then inserts them into the database, handling each symbol sequentially.

    Parameters:
        symbol (str): The stock ticker symbol.
        start_date (str): The start date for the data in 'YYYY-MM-DD' format.
        end_date (str): The end date for the data in 'YYYY-MM-DD' format.
    """
    try:
        # Fetch data
        df = get_historical_prices(symbol, start_date, end_date)
        if df is not None and not df.empty:
            # Insert data into the database immediately after fetching
            await insert_data_to_db(df)

        # Add a delay to avoid hitting rate limits
        await asyncio.sleep(1)  # Adjust this delay as needed
    except Exception as e:
        print(f"Error processing symbol {symbol}: {e}")


async def main(start_date, end_date):
    await db_connector.initialize()
    symbols_list = symbols['symbol'].to_list()  # Assuming symbols is a list of symbols to process

    for symbol in symbols_list:
        await process_symbol(symbol, start_date, end_date)  # Sequentially process each symbol

    await db_connector.close()


# Example usage
if __name__ == "__main__":
    start_date = "1900-01-01"
    end_date = "2025-01-01"
    asyncio.run(main(start_date, end_date))
