import os
import json
import asyncio
import aiofiles
import pandas as pd

from database.async_database import db_connector
from collections import defaultdict
from multiprocessing import Pool, cpu_count

directory_path = "companyfacts"


async def insert_dataframe_to_db(df):
    """Insert a dataframe into the database."""
    # Load symbols and dates tables into memory
    await db_connector.initialize()
    symbols_df = await db_connector.run_query("SELECT symbol_id, cik FROM metadata.symbols", return_df=True)
    dates_df = await db_connector.run_query("SELECT date_id, date FROM metadata.dates", return_df=True)

    # Merge dataframes to resolve foreign keys
    df = df.merge(symbols_df, on='cik', how='left') \
        .merge(dates_df, left_on='start_date', right_on='date', how='left').rename(columns={'date_id': 'start_date_id'}) \
        .merge(dates_df, left_on='end_date', right_on='date', how='left').rename(columns={'date_id': 'end_date_id'}) \
        .merge(dates_df, left_on='filed_date', right_on='date', how='left').rename(columns={'date_id': 'filed_date_id'})

    # Rename columns to match the database schema
    df = df.rename(columns={
        'fy': 'fiscal_year',
        'fp': 'fiscal_period',
        'form': 'form',
        'val': 'value',
        'accn': 'accn'
    })

    # Select relevant columns for insertion
    df = df[['symbol_id', 'fact_name', 'unit', 'start_date_id', 'end_date_id', 'filed_date_id', 'fiscal_year',
             'fiscal_period', 'form', 'value', 'accn']]
    await db_connector.close()
    return df
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
            print(principle, len(df))
            dataframes[principle] = df

        return dataframes


async def main(files):
    all_dataframes = []
    for file in files:
        dataframes = await process_json_file(file)
        all_dataframes.extend(dataframes.values())

    if all_dataframes:
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        dfs_altered = await insert_dataframe_to_db(combined_df)

    return all_dataframes, dfs_altered


if __name__ == "__main__":
    directory_path = "companyfacts"
    files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.json')]
    files = files[0:3]

    dfs, dfs_altered = asyncio.run(main(files))
