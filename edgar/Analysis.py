import pandas as pd
from database.async_database import db_connector


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

    async def calculate_market_cap(self, symbol: str) -> pd.DataFrame:
        """
        Calculate the market capitalization for a given symbol based on the number of outstanding shares
        and the closing price from the historical data.

        Parameters:
        - symbol (str): The stock symbol to calculate the market cap for.

        Returns:
        - pd.DataFrame: DataFrame with the market cap for each date.
        """
        query = """
        CREATE MATERIALIZED VIEW financials.market_caps AS
WITH max_date_cf AS (
    SELECT cf.symbol_id,
           MAX(cf.filed_date_id) AS latest_filed_date_id,
           (SELECT cf_inner.value
            FROM financials.company_facts cf_inner
            WHERE cf_inner.symbol_id = cf.symbol_id
--               AND cf_inner.filed_date_id = MAX(cf.filed_date_id)
              AND cf_inner.end_date_id = MAX(cf.end_date_id)
              AND cf_inner.fact_name IN ('EntityCommonStockSharesOutstanding', 'CommonStockSharesOutstanding')
            LIMIT 1
           ) AS shares_outstanding
    FROM financials.company_facts cf
    WHERE cf.fact_name IN ('EntityCommonStockSharesOutstanding', 'CommonStockSharesOutstanding')
    GROUP BY cf.symbol_id
)
SELECT d.date,
       s.symbol,
       (mcf.shares_outstanding * hd.close) AS market_cap,
       mcf.shares_outstanding as shares,
       hd.close as price
FROM max_date_cf mcf
JOIN financials.historical_data hd ON mcf.symbol_id = hd.symbol_id
JOIN metadata.dates d ON d.date_id = hd.date_id
JOIN metadata.symbols s ON mcf.symbol_id = s.symbol_id
ORDER BY s.symbol, d.date;"""
        return await self.db_connector.run_query(query, params=[symbol], return_df=True)

    async def create_standardized_metrics_view(self):
        """
        Create a materialized view that standardizes financial metrics against market caps and calculates common ratios.
        """
        query = """
        CREATE MATERIALIZED VIEW financials.standardized_metrics AS
        SELECT 
            mc.date,
            mc.symbol,
            mc.market_cap,
            cf.fact_name,
            cf.value / mc.market_cap AS standardized_value,
            CASE WHEN cf.fact_name = 'NetIncomeLoss' THEN cf.value / mc.market_cap END AS net_income_to_market_cap,
            CASE WHEN cf.fact_name = 'StockholdersEquity' THEN cf.value / mc.market_cap END AS equity_to_market_cap,
            CASE WHEN cf.fact_name = 'EarningsPerShareBasic' THEN cf.value / mc.shares END AS eps_basic,
            CASE WHEN cf.fact_name = 'OperatingIncomeLoss' THEN cf.value / mc.market_cap END AS operating_income_to_market_cap,
            CASE WHEN cf.fact_name = 'CashAndCashEquivalentsAtCarryingValue' THEN cf.value / mc.market_cap END AS cash_to_market_cap,
            CASE WHEN cf.fact_name = 'EarningsPerShareDiluted' THEN cf.value / mc.shares END AS eps_diluted,
            CASE WHEN cf.fact_name = 'IncomeTaxExpenseBenefit' THEN cf.value / mc.market_cap END AS tax_expense_to_market_cap,
            CASE WHEN cf.fact_name = 'WeightedAverageNumberOfSharesOutstandingBasic' THEN cf.value END AS shares_outstanding_basic,
            CASE WHEN cf.fact_name = 'WeightedAverageNumberOfDilutedSharesOutstanding' THEN cf.value END AS shares_outstanding_diluted,
            CASE WHEN cf.fact_name = 'Assets' THEN cf.value / mc.market_cap END AS assets_to_market_cap,
            CASE WHEN cf.fact_name = 'LiabilitiesAndStockholdersEquity' THEN cf.value / mc.market_cap END AS liabilities_equity_to_market_cap,
            CASE WHEN cf.fact_name = 'ComprehensiveIncomeNetOfTax' THEN cf.value / mc.market_cap END AS comprehensive_income_to_market_cap,
            CASE WHEN cf.fact_name = 'InterestExpense' THEN cf.value / mc.market_cap END AS interest_expense_to_market_cap,
            CASE WHEN cf.fact_name = 'RetainedEarningsAccumulatedDeficit' THEN cf.value / mc.market_cap END AS retained_earnings_to_market_cap,
            CASE WHEN cf.fact_name = 'NetCashProvidedByUsedInOperatingActivities' THEN cf.value / mc.market_cap END AS cash_flow_operating_to_market_cap,
            CASE WHEN cf.fact_name = 'NetCashProvidedByUsedInFinancingActivities' THEN cf.value / mc.market_cap END AS cash_flow_financing_to_market_cap,
            CASE WHEN cf.fact_name = 'NetCashProvidedByUsedInInvestingActivities' THEN cf.value / mc.market_cap END AS cash_flow_investing_to_market_cap,
            CASE WHEN cf.fact_name = 'CommonStockSharesAuthorized' THEN cf.value END AS shares_authorized,
            CASE WHEN cf.fact_name = 'ShareBasedCompensation' THEN cf.value / mc.market_cap END AS share_based_compensation_to_market_cap,
            CASE WHEN cf.fact_name = 'CommonStockValue' THEN cf.value / mc.market_cap END AS stock_value_to_market_cap,
            CASE WHEN cf.fact_name = 'Liabilities' THEN cf.value / mc.market_cap END AS liabilities_to_market_cap,
            CASE WHEN cf.fact_name = 'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest' THEN cf.value / mc.market_cap END AS income_loss_to_market_cap,
            CASE WHEN cf.fact_name = 'ProfitLoss' THEN cf.value / mc.market_cap END AS profit_loss_to_market_cap,
            CASE WHEN cf.fact_name = 'CommonStockSharesIssued' THEN cf.value END AS shares_issued,
            CASE WHEN cf.fact_name = 'PropertyPlantAndEquipmentNet' THEN cf.value / mc.market_cap END AS ppe_to_market_cap
        FROM financials.company_facts cf
        JOIN financials.market_caps mc ON cf.symbol_id = mc.symbol_id
        WHERE cf.fact_name IN (
            'NetIncomeLoss', 'StockholdersEquity', 'EarningsPerShareBasic', 'OperatingIncomeLoss',
            'CashAndCashEquivalentsAtCarryingValue', 'EarningsPerShareDiluted', 'IncomeTaxExpenseBenefit',
            'WeightedAverageNumberOfSharesOutstandingBasic', 'WeightedAverageNumberOfDilutedSharesOutstanding',
            'Assets', 'LiabilitiesAndStockholdersEquity', 'ComprehensiveIncomeNetOfTax', 'InterestExpense',
            'RetainedEarningsAccumulatedDeficit', 'NetCashProvidedByUsedInOperatingActivities',
            'NetCashProvidedByUsedInFinancingActivities', 'NetCashProvidedByUsedInInvestingActivities',
            'CommonStockSharesAuthorized', 'ShareBasedCompensation', 'CommonStockValue', 'Liabilities',
            'IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest',
            'ProfitLoss', 'CommonStockSharesIssued', 'PropertyPlantAndEquipmentNet'
        );
        """
        await self.db_connector.run_query(query)

    async def most_common_facts(self, top_n: int = 25) -> pd.DataFrame:
        """
        Identify the most commonly reported fact names within the company facts table.

        Parameters:
        - top_n (int): The number of top fact names to return.

        Returns:
        - pd.DataFrame: DataFrame with the most common fact names and their counts.
        """
        query = f"""
        SELECT fact_name, COUNT(*) as count
        FROM financials.company_facts
        GROUP BY fact_name
        ORDER BY count DESC
        LIMIT $1;
        """
        return await self.db_connector.run_query(query, params=[top_n], return_df=True)
