import io
import logging
import os
import json
import asyncio

import aiofiles
import numpy as np
import pandas as pd
from tqdm.asyncio import tqdm as async_tqdm
import aiohttp
import multiprocessing
from database.async_database import db_connector
from database.database import db_connector as normal_connector

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Preload metadata to avoid redundant queries
symbols_df = normal_connector.run_query("SELECT symbol_id, cik FROM metadata.symbols", return_df=True)
dates_df = normal_connector.run_query("SELECT date_id, date FROM metadata.dates", return_df=True)


async def fetch_submission_data(cik: str):
    """Fetch submission data from the SEC endpoint."""
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    headers = {"User-Agent": "Elijah Anderson (elijahanderson96@gmail.com)"}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, ssl=False) as response:
            if response.status == 200:
                return await response.json()
            return None


async def read_json_file(file_path):
    """Read a JSON file asynchronously."""
    async with aiofiles.open(file_path, 'r') as file:
        content = await file.read()
        return json.loads(content)


def process_file_data(data):
    """Process JSON data synchronously (CPU-bound)."""
    cik = str(data.get("cik", "")).zfill(10)
    entity_name = data.get("entityName", "")

    facts = data.get("facts", {})

    records = []
    for principle, facts_dict in facts.items():
        for fact_name, fact_details in facts_dict.items():
            units = fact_details.get("units", {})
            for unit, time_series in units.items():
                for entry in time_series:
                    record = {
                        "cik": cik,
                        "entity_name": entity_name,
                        "fact_name": fact_name,
                        "unit": unit,
                        **entry
                    }
                    records.append(record)
    # Convert to DataFrame
    return pd.DataFrame(records)


def transform_dataframe(df):
    """Transform the DataFrame with required merging and cleaning logic."""
    required_columns = ['cik', 'start', 'end', 'filed', 'fy', 'fp', 'form', 'val', 'accn']
    for col in required_columns:
        if col not in df.columns:
            df[col] = None

    df['cik'] = df['cik'].astype(str)
    df['start'] = pd.to_datetime(df['start'], errors='coerce')
    df['end'] = pd.to_datetime(df['end'], errors='coerce')
    df['filed'] = pd.to_datetime(df['filed'], errors='coerce')

    symbols_df['cik'] = symbols_df['cik'].astype(str)
    dates_df['date'] = pd.to_datetime(dates_df['date'])

    df_merged_symbols = df.merge(symbols_df, on='cik', how='left')

    df_merged_start_date = df_merged_symbols.merge(
        dates_df.rename(columns={'date': 'start'}), on='start', how='left'
    ).rename(columns={'date_id': 'start_date_id'})

    df_merged_end_date = df_merged_start_date.merge(
        dates_df.rename(columns={'date': 'end'}), on='end', how='left'
    ).rename(columns={'date_id': 'end_date_id'})

    df_merged_filed_date = df_merged_end_date.merge(
        dates_df.rename(columns={'date': 'filed'}), on='filed', how='left'
    ).rename(columns={'date_id': 'filed_date_id'})

    df = df_merged_filed_date.rename(columns={
        'fy': 'fiscal_year',
        'fp': 'fiscal_period',
        'form': 'form',
        'val': 'value',
        'accn': 'accn'
    })

    df = df[['symbol_id', 'fact_name', 'unit', 'start_date_id', 'end_date_id', 'filed_date_id',
             'fiscal_year', 'fiscal_period', 'form', 'value', 'accn']]

    df = df.dropna(subset=['symbol_id'])
    df = df.replace({np.nan: None})
    return df


async def insert_dataframe_to_db(df: pd.DataFrame):
    """Insert a dataframe into the database using copy_to_table."""
    output = io.StringIO()
    df.to_csv(output, sep='\t', index=False, header=False, na_rep='\\N')  # PostgreSQL expects '\\N' for NULLs
    output.seek(0)

    async with db_connector.pool.acquire() as connection:
        try:
            logger.info("Starting data insertion using copy_to_table...")
            await connection.copy_to_table(
                'company_facts',
                schema_name='financials',
                source=output,
                format='csv',
                delimiter='\t',
                columns=df.columns.tolist()
            )
            logger.info(f"Successfully inserted {len(df)} records.")
        except Exception as e:
            logger.error(f"Error during copy_to_table operation: {e}")
            raise


async def process_files_in_batches(file_paths, batch_size=1000):
    """Process files in batches for efficiency with multiprocessing and progress tracking."""
    await db_connector.initialize()

    with async_tqdm(total=len(file_paths), desc="Processing files") as pbar:
        for i in range(0, len(file_paths), batch_size):
            batch_files = file_paths[i:i + batch_size]
            combined_data = []

            # Read all JSON files in the batch asynchronously
            file_contents = await asyncio.gather(*[read_json_file(file_path) for file_path in batch_files])
            print('Aggregated file_contents. Beginning multiprocessing...')
            # Use multiprocessing to process the data
            with multiprocessing.Pool(processes=multiprocessing.cpu_count() - 2) as pool:
                processed_dfs = pool.map(process_file_data, file_contents)
            print('Finished multiprocessing.')
            # Combine non-empty DataFrames
            for df in processed_dfs:
                if not df.empty:
                    combined_data.append(df)

                # Update progress bar for each file processed
                pbar.update(1)
            print('Aggregating data.')
            if combined_data:
                # Aggregate all data into a single DataFrame
                aggregated_df = pd.concat(combined_data, ignore_index=True)

                # Transform and insert
                transformed_df = transform_dataframe(aggregated_df)
                print(f'inserting {transformed_df.shape[0]} rows...')
                await insert_dataframe_to_db(transformed_df)

    await db_connector.close()


if __name__ == "__main__":
    directory_path = "/Users/elijahanderson/Downloads/companyfacts"
    files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.json')]

    # Process files in batches
    asyncio.run(process_files_in_batches(files))
