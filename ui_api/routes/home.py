from fastapi import APIRouter, HTTPException
from database.async_database import db_connector
import logging

home_router = APIRouter()

logging.basicConfig(level=logging.INFO)


@home_router.get("/facts", tags=["Facts"])
async def get_latest_facts():
    """
    Fetches the latest data for specified fact names across all symbols.
    """
    fact_names = [
        "Assets",
        "LiabilitiesAndStockholdersEquity",
        "EntityCommonStockSharesOutstanding",
        "CashAndCashEquivalentsAtCarryingValue",
        "RetainedEarningsAccumulatedDeficit",
        "AssetsCurrent",
        "LiabilitiesCurrent",
        "NetIncomeLoss",
        "StockholdersEquity",
        "PropertyPlantAndEquipmentNet",
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInFinancingActivities",
        "CommonStockValue",
        "IncomeTaxExpenseBenefit",
        "NetCashProvidedByUsedInInvestingActivities",
        "OperatingIncomeLoss",
        "EarningsPerShareBasic",
        "CommonStockSharesAuthorized",
        "CommonStockParOrStatedValuePerShare",
        "CommonStockSharesIssued",
        "ShareBasedCompensation",
        "AccumulatedOtherComprehensiveIncomeLossNetOfTax",
        "EarningsPerShareDiluted",
        "WeightedAverageNumberOfSharesOutstandingBasic",
        "AccountsPayableCurrent",
        "Liabilities"
    ]

    try:
        query = f"""
        WITH latest_filing_dates AS (
            SELECT 
                symbol_id, 
                MAX(filing_date_id) AS latest_filing_date_id
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
            latest_filing_dates lfd ON f.symbol_id = lfd.symbol_id AND f.filing_date_id = lfd.latest_filing_date_id
        JOIN 
            metadata.symbols s ON s.symbol_id = f.symbol_id
        JOIN 
            metadata.dates d ON d.date_id = f.filing_date_id
        WHERE 
            f.fact_name = ANY($1)
        ORDER BY 
            s.symbol, f.fact_name;
        """
        result = await db_connector.run_query(query, params=(fact_names,))

        if result.empty:
            raise HTTPException(
                status_code=404,
                detail="No data found for the specified fact names."
            )

        return result.to_dict(orient="records")

    except Exception as e:
        logging.error(f"Error fetching latest facts: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch latest facts.")


@home_router.get("/home", tags=["Home"])
async def get_daily_stock_changes():
    """
    Fetches daily stock price changes (price and percentage change) between the latest two available dates.
    """
    try:
        query = """
        WITH latest_date AS (
            SELECT 
                MAX(hd.date_id) AS current_date_id
            FROM 
                financials.historical_data hd
        ),
        previous_date AS (
            SELECT 
                MAX(hd.date_id) AS previous_date_id
            FROM 
                financials.historical_data hd
            WHERE 
                hd.date_id < (SELECT current_date_id FROM latest_date)
        ),
        current_data AS (
            SELECT 
                s.symbol,
                d.date AS current_date,
                hd.close AS current_close
            FROM 
                financials.historical_data hd
            JOIN 
                metadata.symbols s ON s.symbol_id = hd.symbol_id
            JOIN 
                metadata.dates d ON d.date_id = hd.date_id
            WHERE 
                hd.date_id = (SELECT current_date_id FROM latest_date)
        ),
        previous_data AS (
            SELECT 
                s.symbol,
                d.date AS previous_date,
                hd.close AS previous_close
            FROM 
                financials.historical_data hd
            JOIN 
                metadata.symbols s ON s.symbol_id = hd.symbol_id
            JOIN 
                metadata.dates d ON d.date_id = hd.date_id
            WHERE 
                hd.date_id = (SELECT previous_date_id FROM previous_date)
        )
        SELECT 
            c.symbol,
            c.current_date,
            p.previous_date,
            c.current_close,
            p.previous_close,
            (c.current_close - p.previous_close) AS price_change,
            CASE 
                WHEN p.previous_close = 0 THEN NULL
                ELSE ((c.current_close - p.previous_close) / p.previous_close) * 100
            END AS percentage_change
        FROM 
            current_data c
        JOIN 
            previous_data p ON c.symbol = p.symbol
        ORDER BY 
            c.symbol;
        """
        result = await db_connector.run_query(query)

        if result.empty:
            raise HTTPException(
                status_code=404,
                detail="No data found for daily stock changes."
            )

        return result.to_dict(orient="records")

    except Exception as e:
        logging.error(f"Error fetching daily stock changes: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch daily stock changes.")
