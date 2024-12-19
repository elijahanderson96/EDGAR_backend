import os
import json
import asyncio
import aiofiles
import pandas as pd
from collections import defaultdict
from multiprocessing import Pool, cpu_count

directory_path = "companyfacts"



def extract_keys(data, parent_key=''):
    """Recursively extract keys from a nested dictionary."""
    keys = []
    for key, value in data.items():
        full_key = f"{parent_key}.{key}" if parent_key else key
        keys.append(full_key)
        if isinstance(value, dict):
            keys.extend(extract_keys(value, full_key))
    return keys


async def process_json_file(file_path):
    """Process a single JSON file and return dataframes for each accounting principle."""
    async with aiofiles.open(file_path, 'r') as file:
        content = await file.read()
        data = json.loads(content)
        
        # Extract CIK and entity name
        cik = str(data.get("cik", "")).zfill(10)
        entity_name = data.get("entityName", "")
        
        # Extract facts
        facts = data.get("facts", {})
        
        # Create dataframes for each accounting principle
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


def process_files_in_directory(directory_path):
    """Process all JSON files in the specified directory and aggregate dataframes for each principle."""
    files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.json')]
    
    aggregated_dataframes = defaultdict(list)
    
    for file in files:
        dataframes = asyncio.run(process_json_file(file))
        for principle, df in dataframes.items():
            aggregated_dataframes[principle].append(df)
    
    # Concatenate dataframes for each principle
    for principle in aggregated_dataframes:
        aggregated_dataframes[principle] = pd.concat(aggregated_dataframes[principle], ignore_index=True)
    
    return aggregated_dataframes


if __name__ == "__main__":
    # Example usage
    directory_path = "companyfacts"
    files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.json')]
    
    # Process files and aggregate dataframes
    aggregated_dataframes = process_files_in_directory(directory_path)
    
    for principle, df in aggregated_dataframes.items():
            print(f"Accounting Principle: {principle}")
            print(df.head())  # Display the first few rows of each dataframe
