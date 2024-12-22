import os
import json
import asyncio
import aiofiles
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import cpu_count
import aiohttp

from database.async_database import db_connector

directory_path = "companyfacts"


async def fetch_submission_data(cik: str):
    """Fetch submission data from the SEC endpoint."""
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    headers = {"User-Agent": "Elijah Anderson (elijahanderson96@gmail.com)"}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers, ssl=False) as response:
            if response.status == 200:
                return await response.json()
            else:
                return None


async def insert_dataframe_to_db(df: pd.DataFrame):
    """Insert a dataframe into the database."""
    # Load symbols table into memory
    await db_connector.initialize()
    symbols_df = await db_connector.run_query("SELECT symbol_id, cik FROM metadata.symbols", return_df=True)
    dates_df = await db_connector.run_query("SELECT date_id, date FROM metadata.dates", return_df=True)
    # Check and insert missing CIKs
    for cik in df['cik'].unique():
        if cik not in symbols_df['cik'].values:
            submission_data = await fetch_submission_data(cik)
            if submission_data:
                tickers = submission_data.get("tickers", [])
                symbol = tickers[-1] if tickers else None
                name = submission_data.get("name", "")
                if symbol:
                    print(f"Inserting {cik}...")
                    print(tickers, symbol, name)
                    input("BREAK")
                    await db_connector.run_query(
                        "INSERT INTO metadata.symbols (cik, symbol, title) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;",
                        params=[cik, symbol, name],
                        return_df=False
                    )

    # Cast columns to appropriate types
    df['cik'] = df['cik'].astype(str)
    df['start'] = pd.to_datetime(df['start'])
    df['end'] = pd.to_datetime(df['end'])
    df['filed'] = pd.to_datetime(df['filed'])

    symbols_df['cik'] = symbols_df['cik'].astype(str)
    dates_df['date'] = pd.to_datetime(dates_df['date'])

    df_merged_symbols = df.merge(symbols_df, on='cik', how='left')
    df_merged_start_date = df_merged_symbols.merge(dates_df, left_on='start', right_on='date', how='left').rename(
        columns={'date_id': 'start_date_id'})
    df_merged_end_date = df_merged_start_date.merge(dates_df, left_on='end', right_on='date', how='left').rename(
        columns={'date_id': 'end_date_id'})
    df_merged_filed_date = df_merged_end_date.merge(dates_df, left_on='filed', right_on='date', how='left').rename(
        columns={'date_id': 'filed_date_id'})

    # Rename columns to match the database schema
    df = df_merged_filed_date.rename(columns={
        'fy': 'fiscal_year',
        'fp': 'fiscal_period',
        'form': 'form',
        'val': 'value',
        'accn': 'accn'
    })
    # Select relevant columns for insertion
    df = df[['symbol_id', 'fact_name', 'unit', 'start_date_id', 'end_date_id', 'filed_date_id', 'fiscal_year',
             'fiscal_period', 'form', 'value', 'accn']]

    # Perform bulk insert
    # await db_connector.run_query(
    #     """
    #     INSERT INTO financials.company_facts (
    #         symbol_id, fact_name, unit, start_date_id, end_date_id, filed_date_id,
    #         fiscal_year, fiscal_period, form, value, accn
    #     ) VALUES (
    #         $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
    #     ) ON CONFLICT DO NOTHING;
    #     """,
    #     params=df.values.tolist(),
    #     return_df=False
    # )
    await db_connector.close()
    return df


# AI: If needed, please separate IO bound and CPU bound tasks, we are going to multiprocess this at some point.
# If it's fine to have async mixed with CPI bound tasks you may leave as is. AI!
async def process_json_file(file_path):
    """Process a single JSON file and return dataframes for each accounting principle."""
    async with aiofiles.open(file_path, 'r') as file:
        content = await file.read()
        data = json.loads(content)

        cik = str(data.get("cik", "")).zfill(10)
        entity_name = data.get("entityName", "")

        facts = data.get("facts", {})

        dataframes = {}
        for principle, facts_dict in facts.items():
            records = []
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
            df = pd.DataFrame(records)
            dataframes[principle] = df

        return dataframes


def process_file(file):
    """Wrapper to call the async function from a sync context."""
    return asyncio.run(process_json_file(file))


async def main(files):
    all_dataframes = []
    with ProcessPoolExecutor(max_workers=cpu_count() - 2) as executor:
        for dataframes in tqdm(executor.map(process_file, files), total=len(files), desc="Processing files"):
            all_dataframes.extend(dataframes.values())

    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        df = await insert_dataframe_to_db(combined_df)
        return df


if __name__ == "__main__":
    directory_path = "companyfacts"
    files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.json')]

    df = asyncio.run(main(files[0:3]))
    print(df.shape)
    print(df.sample(25))
