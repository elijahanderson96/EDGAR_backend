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
