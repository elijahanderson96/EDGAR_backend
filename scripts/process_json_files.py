import os
import json
import asyncio
import aiofiles
from multiprocessing import Pool, cpu_count
from database.async_database import db_connector

async def analyze_json_structure(file_path):
    """Analyze the JSON file to determine its structure."""
    async with aiofiles.open(file_path, 'r') as file:
        content = await file.read()
        data = json.loads(content)
        # Assuming the JSON structure is a dictionary
        keys = data.keys()
        # Further analysis can be done here if needed
        return keys

async def process_json_file(file_path):
    """Process a single JSON file and return its keys."""
    async with aiofiles.open(file_path, 'r') as file:
        content = await file.read()
        data = json.loads(content)
        return list(data.keys())

def process_files_in_directory(directory_path):
    """Process all JSON files in the specified directory using multiprocessing."""
    files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.json')]
    files = files[:100]  # Limit to the first 100 files
    with Pool(cpu_count() - 2) as pool:
        keys_list = pool.map(asyncio.run, [process_json_file(file) for file in files])
    return keys_list

if __name__ == "__main__":
    directory_path = "companyfacts"
    process_files_in_directory(directory_path)
