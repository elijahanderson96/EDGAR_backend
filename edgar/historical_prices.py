import io

import pandas as pd
import yfinance as yf
import asyncio
from edgar.symbols import symbols
from database.async_database import db_connector


async def insert_dataframe_to_db(df: pd.DataFrame):
    """Insert a dataframe into the database using copy_to_table."""
    output = io.BytesIO()
    df.to_csv(output, sep='\t', index=False, header=False, na_rep='\\N')
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
            print(e)
            raise


def get_historical_prices(symbols, start_date, end_date):
    """
    Fetches historical end-of-day prices for multiple stock symbols.
    """
    try:
        data = yf.download(symbols, start=start_date, end=end_date, group_by='ticker')
        all_data = []
        for symbol in data.columns.levels[0]:
            symbol_data = data[symbol].copy()
            symbol_data.columns = [col.lower().replace(" ", "_") for col in symbol_data.columns]
            # Drop rows where 'open', 'high', 'low', and 'close' are all NaN
            symbol_data.dropna(subset=['open', 'high', 'low', 'close'], how='all', inplace=True)
            symbol_data['symbol'] = symbol
            all_data.append(symbol_data)
            print(f"Fetched data for {symbol} from {start_date} to {end_date}")
        return pd.concat(all_data)
    except Exception as e:
        print(f"Error fetching data for symbols: {e}")
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
    and performs a bulk insert into the historical_data table using copy_to_table.
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
    insert_df = merged_df[['symbol_id', 'date_id', 'open', 'high', 'low', 'close', 'volume']]
    # Ensure 'volume' and other numeric columns are integers if needed
    if 'volume' in insert_df.columns:
        insert_df['volume'] = insert_df['volume'].fillna(0).astype(int)

    # # Retrieve rows not in the database
    # rows_not_in_db = await db_connector.drop_existing_rows(insert_df[['symbol_id', 'date_id']],
    #                                                        "financials.historical_data", ["symbol_id", "date_id"])

    # # Filter to keep only rows not in the database using `isin`
    # if rows_not_in_db.empty:
    #     print("All rows already exist in the database. No new data to insert.")
    #     return
    #
    # insert_df = insert_df[
    #     (insert_df['symbol_id'].isin(rows_not_in_db['symbol_id']) &
    #      insert_df['date_id'].isin(rows_not_in_db['date_id']))
    # ]

    try:
        # Use insert_dataframe_to_db for bulk insertion
        await insert_dataframe_to_db(insert_df)
        print(f"Inserted {insert_df.shape[0]} records.")
    except Exception as e:
        print(f"Error inserting data for symbols: {e}")


async def process_symbols(symbols, start_date, end_date):
    """
    Fetches historical prices for multiple symbols, aggregates the data, and inserts it into the database.
    """
    try:
        df = get_historical_prices(symbols, start_date, end_date)
        if df is not None and not df.empty:
            # Insert data in chunks of 3 million rows
            chunk_size = 3000000
            for start in range(0, len(df), chunk_size):
                chunk_df = df.iloc[start:start + chunk_size]
                await insert_data_to_db(chunk_df)
    except Exception as e:
        print(f"Error processing symbols: {e}")


async def main(start_date, end_date):
    await db_connector.initialize()
    symbols_list = symbols['symbol'].to_list()  # Assuming symbols is a list of symbols to process
    # Process symbols in batches
    batch_size = 1  # Adjust batch size as needed
    for i in range(0, len(symbols_list), batch_size):
        batch_symbols = symbols_list[i:i + batch_size]
        await process_symbols(batch_symbols, start_date, end_date)

    await db_connector.close()


# Example usage
if __name__ == "__main__":
    start_date = "1900-01-01"
    end_date = "2025-01-01"
    asyncio.run(main(start_date, end_date))
