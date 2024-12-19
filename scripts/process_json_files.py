import os
import json
import asyncio
import aiofiles
from collections import defaultdict
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
    """Process a single JSON file and return its keys and fact groupings."""
    async with aiofiles.open(file_path, 'r') as file:
        content = await file.read()
        data = json.loads(content)
        keys = extract_keys(data)
        
        # Extract fact groupings
        fact_groupings = {}
        facts = data.get('facts', {})
        for group, details in facts.items():
            fact_groupings[group] = list(details.keys())
        
        return keys, fact_groupings


def process_files_in_directory(directory_path):
    """Process all JSON files in the specified directory using multiprocessing."""
    files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.json')]
    files = files[:100]  # Limit to the first 100 files
    keys_list = []
    fact_groupings_list = []
    for file in files:
        keys, fact_groupings = asyncio.run(process_json_file(file))
        keys_list.append(keys)
        fact_groupings_list.append(fact_groupings)
    return keys_list, fact_groupings_list, files


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




keys_list, fact_groupings_list, files = process_files_in_directory(directory_path)
key_counts, subkey_counts = analyze_keys(keys_list)

# Analyze fact groupings
fact_group_counts = defaultdict(int)
fact_counts = defaultdict(lambda: defaultdict(int))

for fact_groupings in fact_groupings_list:
    for group, facts in fact_groupings.items():
        fact_group_counts[group] += 1
        for fact in facts:
            fact_counts[group][fact] += 1

print("Key Counts:", key_counts)
print("Fact Group Counts:", fact_group_counts)
print("Fact Counts:", fact_counts)
print("Subkey Counts:", subkey_counts)

