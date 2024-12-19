import os
import json
import asyncio
import aiofiles
import pandas as pd
from multiprocessing import Pool, cpu_count

directory_path = "companyfacts"


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
    return keys_list, files


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


async def load_data_from_files(files):
    """Load data from a list of JSON files asynchronously."""
    data_list = []
    for file in files:
        async with aiofiles.open(file, 'r') as f:
            content = await f.read()
            data = json.loads(content)
            data_list.append(data)
    return data_list


def create_dataframes_from_facts(data_list):
    """Create separate dataframes for values and metadata of each fact grouping."""
    value_dataframes = {}
    metadata_dataframes = {}

    for data in data_list:
        facts = data.get('facts', {})
        for fact_group, fact_details in facts.items():
            if fact_group not in value_dataframes:
                value_dataframes[fact_group] = []
                metadata_dataframes[fact_group] = []

            for fact_name, fact_info in fact_details.items():
                values = fact_info.get('values', [])
                label = fact_info.get('label')
                description = fact_info.get('description')
                units = fact_info.get('units', {}).get('currency')

                # Append values to the value dataframe
                for value in values:
                    value_dataframes[fact_group].append({
                        'Fact Name': fact_name,
                        'Value': value
                    })

                # Append metadata to the metadata dataframe
                metadata_dataframes[fact_group].append({
                    'Fact Name': fact_name,
                    'Label': label,
                    'Description': description,
                    'Units': units
                })

    # Convert lists to DataFrames
    for group in value_dataframes.keys():
        value_dataframes[group] = pd.DataFrame(value_dataframes[group])
        metadata_dataframes[group] = pd.DataFrame(metadata_dataframes[group])

    return value_dataframes, metadata_dataframes


keys_list, files = process_files_in_directory(directory_path)
key_counts, subkey_counts = analyze_keys(keys_list)
data_list = asyncio.run(load_data_from_files(files))
fact_dataframes = create_dataframes_from_facts(data_list)

print("Key Counts:", key_counts)
print("Subkey Counts:", subkey_counts)

