import pandas as pd
from database.async_database import db_connector
import asyncio


class FactFrequencyAnalyzer:
    def __init__(self):
        self.db_connector = db_connector

    fact_names = set()

    async def fetch_distinct_fact_names(self) -> set:
        """
        Fetch all distinct fact names from the company facts table and set them as a class attribute.

        Returns:
        - set: A set of distinct fact names.
        """
        query = """
        SELECT DISTINCT fact_name
        FROM financials.company_facts;
        """
        result = await self.db_connector.run_query(query, return_df=True)
        self.fact_names = set(result['fact_name'])
        return self.fact_names

    async def check_fact_distribution(self, fact_name: str) -> pd.DataFrame:
        """
        Check which symbols have reported a specific fact and which have not, and determine the percentage of filings
        that include the fact for each reporting company.

        Parameters:
        - fact_name (str): The name of the fact to check.

        Returns:
        - pd.DataFrame: DataFrame with symbols, their reporting status, and the percentage of filings with the fact.
        """
        query = f"""
        WITH reporting_symbols AS (
            SELECT symbol_id,
                   COUNT(DISTINCT filed_date_id) AS fact_count
            FROM financials.company_facts
            WHERE fact_name = $1
            GROUP BY symbol_id
        ),
        total_filings AS (
            SELECT symbol_id,
                   COUNT(DISTINCT filed_date_id) AS total_count
            FROM financials.company_facts
            GROUP BY symbol_id
        )
        SELECT s.symbol,
               CASE WHEN rs.symbol_id IS NOT NULL THEN 'Reported' ELSE 'Not Reported' END AS reporting_status,
               COALESCE((rs.fact_count * 100.0 / tf.total_count), 0) AS filing_percentage
        FROM metadata.symbols s
        LEFT JOIN reporting_symbols rs ON s.symbol_id = rs.symbol_id
        LEFT JOIN total_filings tf ON s.symbol_id = tf.symbol_id
        ORDER BY reporting_status, s.symbol;
        """
        return await self.db_connector.run_query(query, params=[fact_name], return_df=True)

    async def analyze_fact_frequency(self) -> pd.DataFrame:
        """
        Analyze the frequency of each fact name being reported across all symbols.

        Returns:
        - pd.DataFrame: DataFrame with fact names and their reporting frequency percentage.
        """
        query = """
        WITH total_filed_dates AS (
            SELECT COUNT(DISTINCT filed_date_id) AS total_filed_dates
            FROM financials.company_facts
        ),
        total_symbols AS (
            SELECT COUNT(DISTINCT symbol_id) AS total_symbols
            FROM financials.company_facts
        )
        SELECT fact_name,
               (COUNT(DISTINCT filed_date_id) * 100.0 / (SELECT total_filed_dates FROM total_filed_dates)) AS filing_percentage,
               (COUNT(DISTINCT symbol_id) * 100.0 / (SELECT total_symbols FROM total_symbols)) AS symbol_percentage
        FROM financials.company_facts
        GROUP BY fact_name
        ORDER BY filing_percentage DESC, symbol_percentage DESC;
        """
        return await self.run_query(query, return_df=True)

    async def get_common_stock_shares_outstanding(self) -> pd.DataFrame:
        """
        Retrieve the most recent value of CommonStockSharesOutstanding for each symbol.

        Returns:
        - pd.DataFrame: DataFrame with symbols and their most recent CommonStockSharesOutstanding value.
        """
        query = """
        SELECT s.symbol, cf.value AS common_stock_shares_outstanding
        FROM financials.company_facts cf
        JOIN metadata.symbols s ON cf.symbol_id = s.symbol_id
        WHERE cf.fact_name = 'CommonStockSharesOutstanding'
        AND (cf.filed_date_id) = (
            SELECT filed_date_id, MAX(end_date_id)
            FROM financials.company_facts
            WHERE symbol_id = cf.symbol_id
            AND fact_name = 'CommonStockSharesOutstanding'
            GROUP BY filed_date_id
            ORDER BY MAX(cf.end_date_id) DESC
            LIMIT 1
        )
        ORDER BY s.symbol;
        """
        return await self.db_connector.run_query(query, return_df=True)
