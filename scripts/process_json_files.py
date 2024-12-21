import os
import json
import asyncio
import aiofiles
import pandas as pd

from database.async_database import db_connector
from collections import defaultdict
from multiprocessing import Pool, cpu_count

directory_path = "companyfacts"


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


if __name__ == "__main__":
    directory_path = "companyfacts"
    files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.json')]
    files = files[0:3]

    # AI: Please examine the README_NEW.md file to get a feel for the database schema, and use the async db_connector.run_query
    # method to insert the individual dataframes within the compiled list of dfs (or if you think it's more performant
    # you can insert them individually as they're processed. The current file creates a list of dataframes with the following columns:
    # 'cik', 'entity_name', 'fact_name', 'unit', 'end', 'val', 'accn', 'fy', 'fp', 'form', 'filed', 'frame', 'start'.
    # Rename columns as needed, and remove cik and entity_name for symbol_id. Add the unit column to the database setup.py script within company facts.
    # AI!
    dfs = []
    for file in files:
        dataframes = asyncio.run(process_json_file(file))
        dfs.append(dataframes)
        for principle, df in dataframes.items():
            print(f"Accounting Principle: {principle}")
            print(df.head())
