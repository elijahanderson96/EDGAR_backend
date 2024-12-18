import os
import json
import asyncio
import aiofiles
import pandas as pd
from multiprocessing import Pool, cpu_count

async def analyze_json_structure(file_path):
    """Analyze the JSON file to determine its structure."""
    async with aiofiles.open(file_path, 'r') as file:
        content = await file.read()
        data = json.loads(content)
        # Assuming the JSON structure is a dictionary
        keys = data.keys()
        # Further analysis can be done here if needed
        return keys

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
    """Process a single JSON file and return its keys."""
    async with aiofiles.open(file_path, 'r') as file:
        content = await file.read()
        data = json.loads(content)
        return extract_keys(data)

def process_files_in_directory(directory_path):
    """Process all JSON files in the specified directory using multiprocessing."""
    files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.json')]
    files = files[:100]  # Limit to the first 100 files
    keys_list = []
    for file in files:
        keys = asyncio.run(process_json_file(file))
        keys_list.append(keys)
    return keys_list

def analyze_keys(keys_list):
    """Analyze the keys and subkeys to provide counts."""
    from collections import defaultdict

    key_counts = defaultdict(int)
    subkey_counts = defaultdict(lambda: defaultdict(int))

    for keys in keys_list:
        for key in keys:
            parts = key.split('.')
            key_counts[parts[0]] += 1
            if len(parts) > 1:
                subkey_counts[parts[0]][parts[1]] += 1

    return key_counts, subkey_counts

def create_dataframes_from_facts(keys_list):
    """Create dataframes for each fact grouping."""
    fact_dataframes = {}
    
    for keys in keys_list:
        for key in keys:
            parts = key.split('.')
            if parts[0] == 'facts' and len(parts) > 1:
                fact_group = parts[1]
                if fact_group not in fact_dataframes:
                    fact_dataframes[fact_group] = []
                fact_dataframes[fact_group].append(key)

    # Convert lists to DataFrames
    for group, keys in fact_dataframes.items():
        fact_dataframes[group] = pd.DataFrame(keys, columns=['Key'])

    return fact_dataframes
keys = process_files_in_directory(directory_path)
key_counts, subkey_counts = analyze_keys(keys)
fact_dataframes = create_dataframes_from_facts(keys)
print("Key Counts:", key_counts)
print("Subkey Counts:", subkey_counts)
print("Fact DataFrames:")
for group, df in fact_dataframes.items():
    print(f"Group: {group}")
    print(df.head())  # Print the first few rows of each dataframe
