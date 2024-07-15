# main.py
import asyncio
import time
from datetime import datetime

import asyncpg
import pandas as pd
from fastapi import APIRouter

from config.configs import dsn
from database.database import db_connector
from database.async_database import db_connector

benchmark_router = APIRouter()


@benchmark_router.on_event("startup")
async def startup():
    await db_connector.initialize()


@benchmark_router.on_event("shutdown")
async def shutdown():
    await db_connector.close()


@benchmark_router.get("/old_db/benchmark")
def old_db_benchmark():
    start_time = time.time()

    query = """
        SELECT *
        FROM test_table
        LIMIT 100
    """
    results = db_connector.run_query(query)

    duration = time.time() - start_time

    if not results.empty:
        return {
            "duration": duration,
            "results": results.to_dict(orient='records')
        }
    else:
        return {
            "duration": duration,
            "message": "No results found"
        }


@benchmark_router.get("/new_db/benchmark")
async def new_db_benchmark():
    start_time = time.time()

    query = """
        SELECT *
        FROM test_table
        LIMIT 100
    """
    results = await db_connector.run_query(query)

    duration = time.time() - start_time

    if not results.empty:
        return {
            "duration": duration,
            "results": results.to_dict(orient='records')
        }
    else:
        return {
            "duration": duration,
            "message": "No results found"
        }


async def create_table_and_insert_data():
    pool = await asyncpg.create_pool(dsn=dsn)
    async with pool.acquire() as connection:
        await connection.execute("""
            DROP TABLE IF EXISTS test_table;
            CREATE TABLE test_table (
                id SERIAL PRIMARY KEY,
                column1 TEXT,
                column2 TEXT,
                column3 TEXT
            );
        """)

        # Generate 3 million rows of data
        num_rows = 3_000_000
        data = {
            "column1": [f"text_{i}" for i in range(num_rows)],
            "column2": [f"text_{i}" for i in range(num_rows)],
            "column3": [f"text_{i}" for i in range(num_rows)],
        }
        df = pd.DataFrame(data)

        # Insert data in chunks to avoid memory issues
        chunk_size = 10000
        for start in range(0, num_rows, chunk_size):
            chunk = df.iloc[start:start + chunk_size]
            values = [tuple(row) for row in chunk.itertuples(index=False, name=None)]
            await connection.executemany(
                "INSERT INTO test_table (column1, column2, column3) VALUES ($1, $2, $3)",
                values
            )

    await pool.close()

# Run the script to create the table and insert data
# asyncio.run(create_table_and_insert_data())
