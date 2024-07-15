import asyncpg
import pandas as pd
from typing import Union, Dict, Optional, Any
import logging
from config.configs import dsn


class AsyncpgConnector:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None
        self.logger = logging.getLogger(__name__)

    async def initialize(self):
        self.pool = await asyncpg.create_pool(dsn=self.dsn)

    async def close(self):
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

        Args:
            query (Union[str, asyncpg.Record]): SQL query as a string or asyncpg Record object.
            params (Optional[Dict], optional): Dictionary of parameters to use in the query. Defaults to None.
            return_df (bool, optional): Whether to return the results as a pandas DataFrame.
                                        Defaults to True. If False, None is returned.
            fetch_one (bool, optional): Whether to return a single value.
                                        Defaults to False. If True, returns a single value from the query result.

        Returns:
            Optional[Union[pd.DataFrame, Any]]: Result of the query as a pandas DataFrame, if return_df is True and
            the query retrieves data. Otherwise, None is returned. If fetch_one is True, returns a single value from the query
            result.
        """
        try:
            async with self.pool.acquire() as connection:
                if fetch_one:
                    result = await connection.fetchrow(query, *params) if params else await connection.fetch(query)
                    return result[0] if result else None
                else:
                    result = await connection.fetch(query, *params) if params else await connection.fetch(query)
                    if return_df:
                        data = [dict(record) for record in result]
                        return pd.DataFrame(data)
                    return None
        except Exception as e:
            self.logger.error(f"Error occurred while executing query: {e}")
            raise e


db_connector = AsyncpgConnector(dsn=dsn)
