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

async def process_json_file(file_path, db_connector):
    """Process a single JSON file and insert data into the database."""
    async with aiofiles.open(file_path, 'r') as file:
        content = await file.read()
        data = json.loads(content)
        return data
        # Process data and prepare for database insertion
        # Example: Flatten the JSON structure or extract necessary fields
        # Insert data into the database
        #await db_connector.run_query("INSERT INTO your_table (column1, column2) VALUES ($1, $2)", [data['key1'], data['key2']])

def process_files_in_directory(directory_path):
    """Process all JSON files in the specified directory using multiprocessing."""
    files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) if f.endswith('.json')]
    db_connector.initialize()
    with Pool(cpu_count() - 2) as pool:
        pool.starmap(asyncio.run, [(process_json_file(file, db_connector)) for file in files])
    db_connector.close()

if __name__ == "__main__":
    directory_path = "companyfacts"
    process_files_in_directory(directory_path)
