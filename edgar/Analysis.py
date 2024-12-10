import pandas as pd
from database.async_database import db_connector


class FactFrequencyAnalyzer:
    def __init__(self):
        self.db_connector = db_connector

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
        return await self.db_connector.run_query(query, return_df=True)


analyzer = FactFrequencyAnalyzer()
result = analyzer.analyze_fact_frequency()
