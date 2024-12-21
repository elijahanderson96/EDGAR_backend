import os
import json
import asyncio
import aiofiles
import pandas as pd

from database.async_database import db_connector
from collections import defaultdict
from multiprocessing import Pool, cpu_count

directory_path = "companyfacts"

async def insert_dataframe_to_db(df, principle):
    """Insert a dataframe into the database."""
    for _, row in df.iterrows():
        query = """
        INSERT INTO financials.company_facts (
            symbol_id, fact_name, unit, start_date_id, end_date_id, filed_date_id,
            fiscal_year, fiscal_period, form, value, accn
        ) VALUES (
            (SELECT symbol_id FROM metadata.symbols WHERE cik = $1),
            $2, $3, 
            (SELECT date_id FROM metadata.dates WHERE date = $4),
            (SELECT date_id FROM metadata.dates WHERE date = $5),
            (SELECT date_id FROM metadata.dates WHERE date = $6),
            $7, $8, $9, $10, $11
        ) ON CONFLICT DO NOTHING;
        """
        await db_connector.run_query(
            query,
            params=[
                row['cik'], row['fact_name'], row['unit'], row['start'],
                row['end'], row['filed'], row['fy'], row['fp'], row['form'],
                row['val'], row['accn']
            ],
            return_df=False
        )


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
                        print(record)
            df = pd.DataFrame(records)
            dataframes[principle] = df
        
        return dataframes


async def main(files):
    for file in files:
        dataframes = await process_json_file(file)
        for principle, df in dataframes.items():
            print(f"Accounting Principle: {principle}")
            print(df.head())
            await insert_dataframe_to_db(df, principle)

if __name__ == "__main__":
    directory_path = "companyfacts"
    files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.json')]
    files = files[0:3]

    asyncio.run(main(files))
