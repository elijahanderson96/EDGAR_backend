"""CREATE MATERIALIZED VIEW latest_company_facts AS
WITH latest_filing_dates AS (
    SELECT
        symbol_id,
        MAX(filed_date_id) AS latest_filed_date_id
    FROM
        financials.company_facts
    GROUP BY
        symbol_id
)
SELECT
    s.symbol,
    f.fact_name,
    f.value,
    d.date AS filing_date
FROM
    financials.company_facts f
JOIN
    latest_filing_dates lfd
    ON f.symbol_id = lfd.symbol_id AND f.filed_date_id = lfd.latest_filed_date_id
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
    s.symbol, f.fact_name;"""
