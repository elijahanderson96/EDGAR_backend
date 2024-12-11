import logging
import time
from database.async_database import db_connector

def log_execution_time(func):
    async def wrapper(*args, **kwargs):
        logger = logging.getLogger(__name__)
        start_time = time.time()
        logger.info(f"Starting '{func.__name__}' execution.")
        result = await func(*args, **kwargs)
        end_time = time.time()
        logger.info(f"Finished '{func.__name__}' execution in {end_time - start_time:.2f} seconds.")
        return result
    return wrapper


class MaterializedFinancialsViews:
    @staticmethod
    @log_execution_time
    async def generate_latest_facts_view():
        query = """
        CREATE MATERIALIZED VIEW latest_company_facts AS
        WITH latest_filing_dates AS (
            SELECT
                symbol_id,
                fact_name,
                MAX(filed_date_id) AS latest_filed_date_id
            FROM
                financials.company_facts
            GROUP BY
                symbol_id, fact_name
        ),
        latest_facts AS (
            SELECT
                f.symbol_id,
                f.fact_name,
                MAX(f.start_date_id) AS latest_start_date_id,
                MAX(f.end_date_id) AS latest_end_date_id,
                MAX(f.filed_date_id) AS latest_filed_date_id
            FROM
                financials.company_facts f
            JOIN
                latest_filing_dates lfd
                ON f.symbol_id = lfd.symbol_id
                AND f.fact_name = lfd.fact_name
                AND f.filed_date_id = lfd.latest_filed_date_id
            GROUP BY
                f.symbol_id, f.fact_name
        )
        SELECT
            s.symbol,
            f.fact_name,
            f.value,
            d.date AS filing_date
        FROM
            financials.company_facts f
        JOIN
            latest_facts lf
            ON f.symbol_id = lf.symbol_id
            AND f.fact_name = lf.fact_name
            AND f.start_date_id = lf.latest_start_date_id
            AND f.end_date_id = lf.latest_end_date_id
            AND f.filed_date_id = lf.latest_filed_date_id
        JOIN
            metadata.symbols s
            ON s.symbol_id = f.symbol_id
        JOIN
            metadata.dates d
            ON d.date_id = f.filed_date_id
        WHERE
            f.fact_name IN (
                'Assets',
                'LiabilitiesAndStockholdersEquity',
                'EntityCommonStockSharesOutstanding',
                'CashAndCashEquivalentsAtCarryingValue',
                'RetainedEarningsAccumulatedDeficit',
                'AssetsCurrent',
                'LiabilitiesCurrent',
                'NetIncomeLoss',
                'StockholdersEquity',
                'PropertyPlantAndEquipmentNet',
                'NetCashProvidedByUsedInOperatingActivities',
                'NetCashProvidedByUsedInFinancingActivities',
                'CommonStockValue',
                'IncomeTaxExpenseBenefit',
                'NetCashProvidedByUsedInInvestingActivities',
                'OperatingIncomeLoss',
                'EarningsPerShareBasic',
                'CommonStockSharesAuthorized',
                'CommonStockParOrStatedValuePerShare',
                'CommonStockSharesIssued',
                'ShareBasedCompensation',
                'AccumulatedOtherComprehensiveIncomeLossNetOfTax',
                'EarningsPerShareDiluted',
                'WeightedAverageNumberOfSharesOutstandingBasic',
                'AccountsPayableCurrent',
                'Liabilities',
                'WeightedAverageNumberOfDilutedSharesOutstanding',
                'InterestExpense',
                'PaymentsToAcquirePropertyPlantAndEquipment',
                'CommonStockSharesOutstanding',
                'OtherAssetsNoncurrent',
                'InventoryNet',
                'AccountsReceivableNetCurrent',
                'IncreaseDecreaseInInventories',
                'Goodwill',
                'AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment',
                'GrossProfit',
                'SellingGeneralAndAdministrativeExpense',
                'AdditionalPaidInCapital',
                'DeferredIncomeTaxExpenseBenefit',
                'PreferredStockSharesAuthorized',
                'IncreaseDecreaseInAccountsReceivable',
                'OtherLiabilitiesNoncurrent',
                'ComprehensiveIncomeNetOfTax',
                'Revenues',
                'AccruedLiabilitiesCurrent',
                'PreferredStockParOrStatedValuePerShare',
                'OperatingExpenses',
                'IncreaseDecreaseInAccountsPayable',
                'DepreciationDepletionAndAmortization',
                'OtherNonoperatingIncomeExpense'
            )
        ORDER BY
            s.symbol, f.fact_name;
        """
        await db_connector.run_query(query, return_df=False)
