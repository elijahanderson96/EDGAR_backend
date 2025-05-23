import pandas as pd
from typing import Any, Dict, List, Optional, Union
import logging
from config.configs import dsn
import asyncpg


class AsyncpgConnector:
    def __init__(self, dsn: str, min_size: int = 3, max_size: int = 5):
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self.logger = logging.getLogger(__name__)
        self.pool = None

    async def initialize(self):
        """Initialize the connection pool."""
        try:
            self.pool = await asyncpg.create_pool(
                dsn=self.dsn,
                min_size=self.min_size,
                max_size=self.max_size
            )
        except Exception as e:
            self.logger.error(f"Error occurred while creating connection pool: {e}")
            raise e

    async def close(self):
        """Close the connection pool."""
        if self.pool:
            await self.pool.close()

    async def run_query(
            self,
            query: Union[str, asyncpg.Record],
            params: Optional[Dict] = None,
            return_df: bool = True,
            fetch_one: bool = False,
    ) -> Optional[Union[pd.DataFrame, Any]]:
        """
        Execute a query on the database.
        """
        # Ensure pool is initialized before acquiring connection
        if not self.pool:
            self.logger.error("Connection pool is not initialized.")
            # Depending on desired behavior, either raise an error or try to initialize
            # For now, raise an error to make the issue explicit
            raise RuntimeError("Database connection pool has not been initialized.")

        async with self.pool.acquire() as connection:
            try:
                if fetch_one:
                    # fetchrow returns a single Record or None
                    result = await connection.fetchrow(query, *params) if params else await connection.fetchrow(query)
                    # Return the record directly (acts like a dict) or None
                    return result
                else:
                    # fetch returns a list of Records
                    result = await connection.fetch(query, *params) if params else await connection.fetch(query)
                    if return_df:
                        data = [dict(record) for record in result]
                        return pd.DataFrame(data)
                    # Return the list of asyncpg.Record objects if return_df is False
                    return result
            except Exception as e:
                self.logger.error(f"Error occurred while executing query: {e}")
                raise e

    async def drop_existing_rows(
            self,
            df: pd.DataFrame,
            table_name: str,
            unique_key_columns: List[str]
    ) -> pd.DataFrame:
        """
        Checks if rows in a DataFrame exist in a specified database table
        and drops duplicates in the DataFrame.

        Parameters:
        - df (pd.DataFrame): DataFrame with rows to check.
        - table_name (str): Name of the table to check against.
        - unique_key_columns (List[str]): List of column names that define uniqueness.

        Returns:
        - pd.DataFrame: DataFrame with rows that do not exist in the database table.
        """
        # Drop duplicates based on unique columns
        df = df.drop_duplicates(subset=unique_key_columns)

        # Prepare values list for SQL query, converting None to SQL NULL
        values_list = ', '.join(
            f"({', '.join('NULL' if pd.isna(val) else repr(val) for val in row)})"
            for row in df[unique_key_columns].itertuples(index=False, name=None)
        )
        values_clause = f"({', '.join(unique_key_columns)})"

        # SQL query to find existing rows
        query = f"""
        WITH temp_df_check AS (
            SELECT * FROM (VALUES {values_list}) AS temp {values_clause}
        )
        SELECT t.*
        FROM temp_df_check AS temp
        JOIN {table_name} AS t
        ON {" AND ".join([f"temp.{col} = t.{col}" for col in unique_key_columns])}
        """

        async with self.pool.acquire() as connection:
            try:
                # Execute the query and fetch existing rows
                existing_rows = await connection.fetch(query)

                # If we have existing rows, filter them out from the DataFrame
                if existing_rows:
                    existing_df = pd.DataFrame([dict(row) for row in existing_rows])
                    # Merge to identify and keep only non-existing rows
                    df = df.merge(existing_df, on=unique_key_columns, how='left', indicator=True)
                    df = df[df['_merge'] == 'left_only'].drop(columns='_merge')
            except Exception as e:
                self.logger.error(f"Error occurred while checking and dropping existing rows: {e}")
                raise e

        return df


db_connector = AsyncpgConnector(dsn=dsn)
