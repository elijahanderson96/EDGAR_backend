import pandas as pd
from typing import Dict

from database.async_database import db_connector


async def get_table_column_types(schema: str, table: str) -> Dict[str, str]:
    """
    Fetches the column names and data types from a PostgreSQL table.

    Args:
        schema (str): The schema name (e.g., 'financials').
        table (str): The table name (e.g., 'historical_data').

    Returns:
        Dict[str, str]: A dictionary mapping column names to PostgreSQL data types.
    """
    query = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{schema}' AND table_name = '{table}';
    """

    async with db_connector.pool.acquire() as connection:
        try:
            result = await connection.fetch(query)
            return {row['column_name']: row['data_type'] for row in result}
        except Exception as e:
            print(f"🚨 Error fetching table schema: {e}")
            return {}

def map_postgres_to_pandas(postgres_type: str):
    """
    Maps PostgreSQL data types to corresponding pandas data types.

    Args:
        postgres_type (str): The PostgreSQL data type.

    Returns:
        A pandas-compatible data type.
    """
    mapping = {
        'integer': 'Int64',
        'bigint': 'Int64',
        'smallint': 'Int64',
        'numeric': 'float64',
        'double precision': 'float64',
        'real': 'float64',
        'text': 'string',
        'varchar': 'string',
        'character varying': 'string',
        'boolean': 'boolean',
        'date': 'datetime64[ns]',
        'timestamp without time zone': 'datetime64[ns]',
        'timestamp with time zone': 'datetime64[ns]'
    }
    return mapping.get(postgres_type, 'object')  # Default to object if type is unknown

async def cast_dataframe_to_table_schema(df: pd.DataFrame, schema: str, table: str) -> pd.DataFrame:
    """
    Casts a DataFrame to match the PostgreSQL table's column data types.

    Args:
        df (pd.DataFrame): The DataFrame to cast.
        schema (str): The schema name (e.g., 'financials').
        table (str): The table name (e.g., 'historical_data').

    Returns:
        pd.DataFrame: The DataFrame with columns cast to the appropriate data types.
    """
    column_types = await get_table_column_types(schema, table)

    for col, pg_type in column_types.items():
        if col in df.columns:
            try:
                pandas_type = map_postgres_to_pandas(pg_type)
                df[col] = df[col].astype(pandas_type)
                #print(f"✅ Successfully cast '{col}' to {pandas_type}")
            except Exception as e:
                print(f"🚨 Error casting column '{col}' to {pandas_type}: {e}")

    return df
