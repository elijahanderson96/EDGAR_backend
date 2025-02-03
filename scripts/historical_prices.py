import io
from asyncio import sleep
import argparse
import pandas as pd
import yfinance as yf
import asyncio
from edgar.symbols import symbols
from database.async_database import db_connector
from utils.type_matcher import cast_dataframe_to_table_schema


async def insert_dataframe_to_db(df: pd.DataFrame):
    df = await db_connector.drop_existing_rows(df, 'financials.historical_data', df.columns.values.tolist())

    # ✅ Drop unnecessary ID column
    if 'id' in df.columns:
        df.drop(labels=['id'], inplace=True, axis='columns')

    df = await cast_dataframe_to_table_schema(df, 'financials', 'historical_data')
    output = io.BytesIO()

    # ✅ Use 'NULL' instead of '\N'
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
        except Exception as e:
            print(f"🚨 Error inserting into database: {e}")
            raise


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
            print(f"✅ Fetched data for {symbol} from {start_date} to {end_date}")

        return pd.concat(all_data) if all_data else pd.DataFrame()

    except Exception as e:
        print(f"🚨 Error fetching data for symbols: {e}")
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
        print(f"🚨 Error fetching metadata: {e}")
        return None, None


async def insert_data_to_db(df):
    """
    Merges the historical price data with symbol and date metadata,
    and performs a bulk insert into the historical_data table using copy_to_table.
    """
    symbols_df, dates_df = await fetch_metadata()
    if symbols_df is None or dates_df is None:
        print("🚨 Skipping data insertion due to missing metadata.")
        return

    df = df.copy()  # Ensure it's a copy to avoid SettingWithCopyWarning
    df['date'] = pd.to_datetime(df.index)

    dates_df = dates_df.copy()
    dates_df['date'] = pd.to_datetime(dates_df['date'])

    # Merge with metadata
    merged_df = df.merge(symbols_df, on='symbol', how='inner')
    merged_df = merged_df.merge(dates_df, on='date', how='inner')

    # Select relevant columns for insertion
    insert_df = merged_df[['symbol_id', 'date_id', 'open', 'high', 'low', 'close', 'volume']].copy()

    # Ensure volume is properly formatted
    if 'volume' in insert_df.columns:
        insert_df.loc[:, 'volume'] = insert_df['volume'].fillna(0).astype(float).astype(int)

    try:
        await insert_dataframe_to_db(insert_df)
        print(f"✅ Inserted {insert_df.shape[0]} records into the database.")
    except Exception as e:
        print(f"🚨 Error inserting data into database: {e}")


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
        print(f"🚨 Error processing symbols: {e}")


async def main(start_date, end_date):
    await db_connector.initialize()

    symbols_list = symbols['symbol'].to_list()
    batch_size = 10

    for i in range(0, len(symbols_list), batch_size):
        await sleep(3)
        batch_symbols = symbols_list[i:i + batch_size]
        await process_symbols(batch_symbols, start_date, end_date)

    await db_connector.close()


# Assuming your existing `main()` function is defined somewhere
# async def main(start_date, end_date):
#     # Your existing logic
#     pass

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
        required=True,
        help="End date in YYYY-MM-DD format (e.g., 2025-01-01)"
    )

    return parser.parse_args()


if __name__ == "__main__":
    # Usage below:
    # python fetch_data.py --start-date 2000-01-01 --end-date 2025-01-01
    args = parse_arguments()
    asyncio.run(main(args.start_date, args.end_date))
