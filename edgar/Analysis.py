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

    async def analyze_fact_reporting(self, fact_name: str) -> pd.DataFrame:
        """
        Analyze which companies report a specific fact.

        Parameters:
        - fact_name (str): The name of the fact to analyze.

        Returns:
        - pd.DataFrame: DataFrame with symbols that report and do not report the fact.
        """
        query = f"""
        WITH reporting_symbols AS (
            SELECT DISTINCT symbol_id
            FROM financials.company_facts
            WHERE fact_name = $1
        )
        SELECT s.symbol,
               CASE WHEN rs.symbol_id IS NOT NULL THEN 'Reported' ELSE 'Not Reported' END AS reporting_status
        FROM metadata.symbols s
        LEFT JOIN reporting_symbols rs ON s.symbol_id = rs.symbol_id
        ORDER BY reporting_status, s.symbol;
        """
        return await self.db_connector.run_query(query, params=[fact_name], return_df=True)

    async def analyze_fact_frequency(self) -> pd.DataFrame:
        """
        Analyze the frequency of facts reported by companies.

        Returns:
        - pd.DataFrame: DataFrame with fact names and their reporting frequency.
        """
        query = """
        SELECT fact_name, COUNT(*) as frequency
        FROM financials.company_facts
        GROUP BY fact_name
        ORDER BY frequency DESC;
        """
        return await self.db_connector.run_query(query, return_df=True)


analyzer = FactFrequencyAnalyzer()


async def main():
    await analyzer.db_connector.initialize()
    await analyzer.fetch_distinct_fact_names()
    print("Distinct Fact Names:", analyzer.fact_names)

    try:
        result = await analyzer.analyze_fact_frequency()
        print("Fact Frequency Analysis:\n", result)

        # Example usage of analyze_fact_reporting
        fact_name = next(iter(analyzer.fact_names))  # Just an example, using the first fact name
        reporting_result = await analyzer.analyze_fact_reporting(fact_name)
        print(f"Reporting Analysis for {fact_name}:\n", reporting_result)
    finally:
        await analyzer.db_connector.close()

    return result


df = asyncio.run(main())
