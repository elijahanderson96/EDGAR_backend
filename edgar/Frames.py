import asyncio

import pandas as pd
import requests
from database.async_database import db_connector  # Assume this is an instance of AsyncpgConnector
from edgar.Utils import generate_quarterly_periods


class SECFramesDataFetcher:
    BASE_URL = "https://data.sec.gov/api/xbrl/frames"
    HEADERS = {
        "User-Agent": "Elijah Anderson (elijahanderson96@gmail.com)"
    }

    def __init__(self, tag: str, currency: str = "USD", period: str = "CY2023Q1"):
        self.tag = tag
        self.currency = currency
        self.period = period
        print(self.period)

    def _construct_url(self) -> str:
        return f"{self.BASE_URL}/{self.tag}/{self.currency}/{self.period}.json"

    def fetch_data(self) -> dict:
        url = self._construct_url()
        print(url)
        response = requests.get(url, headers=self.HEADERS)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            return {}

    def get_results(self) -> pd.DataFrame:
        data = self.fetch_data()
        if 'data' not in data:
            return pd.DataFrame()

        df = pd.DataFrame(data['data'])
        df['cik'] = df['cik'].astype(str).str.zfill(10)
        return df

    async def insert_into_db(self, table_name: str):
        try:
            df = self.get_results()

            if df.empty:
                print(f"No data available for insertion into {table_name}.")
                return

            df = df.rename(columns={"start": "start_date", "end": "end_date", "val": "value", "accn": "accn"})
            is_instant_data = "start_date" not in df.columns or df["start_date"].isnull().all()

            if is_instant_data:
                df = df[['cik', 'accn', 'end_date', 'value']]
            else:
                df = df[['cik', 'accn', 'start_date', 'end_date', 'value']]

            if "start_date" in df.columns:
                df['start_date'] = pd.to_datetime(df['start_date']).dt.date
            df['end_date'] = pd.to_datetime(df['end_date']).dt.date

            symbol_map = await db_connector.run_query("SELECT cik, symbol_id FROM metadata.symbols")
            cik_to_symbol_id = dict(zip(symbol_map['cik'], symbol_map['symbol_id']))
            df['symbol_id'] = df['cik'].map(cik_to_symbol_id)
            df = df.dropna(subset=['symbol_id'])
            df['symbol_id'] = df['symbol_id'].astype(int)

            date_map = await db_connector.run_query("SELECT date_id, date FROM metadata.dates")
            date_to_date_id = dict(zip(pd.to_datetime(date_map['date']).dt.date, date_map['date_id']))
            df['start_date_id'] = df['start_date'].map(date_to_date_id) if not is_instant_data else None
            df['end_date_id'] = df['end_date'].map(date_to_date_id)
            df = df.dropna(subset=['end_date_id'])

            columns_to_insert = ['symbol_id', 'accn', 'start_date_id', 'end_date_id', 'value'] \
                if not is_instant_data else ['symbol_id', 'accn', 'end_date_id', 'value']

            # Check for duplicates
            df_filtered = await db_connector.drop_existing_rows(df, f"financials.{table_name}",
                                                                unique_key_columns=['symbol_id', 'accn', 'end_date_id']
                                                                if is_instant_data else ['symbol_id', 'accn',
                                                                                         'start_date_id',
                                                                                         'end_date_id'])

            if df_filtered.empty:
                print(f"All rows already exist in {table_name}. No new data to insert.")
                return

            records = df_filtered[columns_to_insert].values.tolist()
            insert_query = f"""
                INSERT INTO financials.{table_name} ({', '.join(columns_to_insert)})
                VALUES ({', '.join([f'${i + 1}' for i in range(len(columns_to_insert))])})
            """

            await db_connector.pool.executemany(insert_query, records)
            print(f"Inserted data into {table_name}")

        except Exception as e:
            print(f"Error inserting data into {table_name}: {e}")


class RevenueFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/Revenues", currency=currency, period=period)

    async def insert_revenue(self):
        await self.insert_into_db("revenue")


class AssetsFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1I"):
        period += 'I' if not period.endswith('I') else period
        super().__init__(tag="us-gaap/Assets", currency=currency, period=period)

    async def insert_assets(self):
        await self.insert_into_db("assets")


class LiabilitiesFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1I"):
        period += 'I' if not period.endswith('I') else period
        super().__init__(tag="us-gaap/Liabilities", currency=currency, period=period)

    async def insert_liabilities(self):
        await self.insert_into_db("liabilities")


class EarningsFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/NetIncomeLoss", currency=currency, period=period)

    async def insert_earnings(self):
        await self.insert_into_db("net_income_loss")


class EPSBasicFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD-per-shares", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/EarningsPerShareBasic", currency=currency, period=period)

    async def insert_eps_basic(self):
        await self.insert_into_db("eps_basic")


class EPSDilutedFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD-per-shares", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/EarningsPerShareDiluted", currency=currency, period=period)

    async def insert_eps_diluted(self):
        await self.insert_into_db("eps_diluted")


class CashOperatingActivitiesFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/NetCashProvidedByUsedInOperatingActivities", currency=currency, period=period)

    async def insert_cash_operating_activities(self):
        await self.insert_into_db("cash_operating_activities")


class CashInvestingActivitiesFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/NetCashProvidedByUsedInInvestingActivities", currency=currency, period=period)

    async def insert_cash_investing_activities(self):
        await self.insert_into_db("cash_investing_activities")


class CashFinancingActivitiesFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/NetCashProvidedByUsedInFinancingActivities", currency=currency, period=period)

    async def insert_cash_financing_activities(self):
        await self.insert_into_db("cash_financing_activities")


class CurrentAssetsFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1I"):
        period += 'I' if not period.endswith('I') else period
        super().__init__(tag="us-gaap/AssetsCurrent", currency=currency, period=period)

    async def insert_current_assets(self):
        await self.insert_into_db("current_assets")


class CurrentLiabilitiesFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1I"):
        period += 'I' if not period.endswith('I') else period
        super().__init__(tag="us-gaap/LiabilitiesCurrent", currency=currency, period=period)

    async def insert_current_liabilities(self):
        await self.insert_into_db("current_liabilities")


class GrossProfitFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/GrossProfit", currency=currency, period=period)

    async def insert_gross_profit(self):
        await self.insert_into_db("gross_profit")


class OperatingIncomeFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/OperatingIncomeLoss", currency=currency, period=period)

    async def insert_operating_income(self):
        await self.insert_into_db("operating_income")


class RetainedEarningsFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1I"):
        period += 'I' if not period.endswith('I') else period
        super().__init__(tag="us-gaap/RetainedEarningsAccumulatedDeficit", currency=currency, period=period)

    async def insert_retained_earnings(self):
        await self.insert_into_db("retained_earnings")


class CommonStockFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1I"):
        period += 'I' if not period.endswith('I') else period
        super().__init__(tag="us-gaap/CommonStockValue", currency=currency, period=period)

    async def insert_common_stock(self):
        await self.insert_into_db("common_stock")


class PreferredStockFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1I"):
        period += 'I' if not period.endswith('I') else period
        super().__init__(tag="us-gaap/PreferredStockValue", currency=currency, period=period)

    async def insert_preferred_stock(self):
        await self.insert_into_db("preferred_stock")


class InterestExpenseFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/InterestExpense", currency=currency, period=period)

    async def insert_interest_expense(self):
        await self.insert_into_db("interest_expense")


class DepreciationAndAmortizationFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/DepreciationAndAmortization", currency=currency, period=period)

    async def insert_depreciation_and_amortization(self):
        await self.insert_into_db("depreciation_and_amortization")


class CostOfRevenueFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/CostOfRevenue", currency=currency, period=period)

    async def insert_cost_of_revenue(self):
        await self.insert_into_db("cost_of_revenue")


class OperatingExpensesFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/OperatingExpenses", currency=currency, period=period)

    async def insert_operating_expenses(self):
        await self.insert_into_db("operating_expenses")


class InventoryFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1I"):
        period += 'I' if not period.endswith('I') else period
        super().__init__(tag="us-gaap/InventoryNet", currency=currency, period=period)

    async def insert_inventory(self):
        await self.insert_into_db("inventory")


class PropertyPlantAndEquipmentFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1I"):
        period += 'I' if not period.endswith('I') else period
        super().__init__(tag="us-gaap/PropertyPlantAndEquipmentNet", currency=currency, period=period)

    async def insert_property_plant_and_equipment(self):
        await self.insert_into_db("property_plant_and_equipment")


class GoodwillFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1I"):
        period += 'I' if not period.endswith('I') else period
        super().__init__(tag="us-gaap/Goodwill", currency=currency, period=period)

    async def insert_goodwill(self):
        await self.insert_into_db("goodwill")


class IntangibleAssetsFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1I"):
        period += 'I' if not period.endswith('I') else period
        super().__init__(tag="us-gaap/IntangibleAssetsNetExcludingGoodwill", currency=currency, period=period)

    async def insert_intangible_assets(self):
        await self.insert_into_db("intangible_assets")


class TotalStockholdersEquityFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1I"):
        period += 'I' if not period.endswith('I') else period
        super().__init__(tag="us-gaap/StockholdersEquity", currency=currency, period=period)

    async def insert_total_stockholders_equity(self):
        await self.insert_into_db("total_stockholders_equity")


class ResearchAndDevelopmentExpenseFetcher(SECFramesDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/ResearchAndDevelopmentExpense", currency=currency, period=period)

    async def insert_research_and_development_expense(self):
        await self.insert_into_db("research_and_development_expense")


async def fetcher():
    await db_connector.initialize()

    # Generate all quarterly periods from 2010 to the current date
    periods = generate_quarterly_periods()

    # List of all fetcher classes with their respective insert methods
    fetchers = [
        (RevenueFetcher, "insert_revenue"),
        (AssetsFetcher, "insert_assets"),
        (LiabilitiesFetcher, "insert_liabilities"),
        (EarningsFetcher, "insert_earnings"),
        (EPSBasicFetcher, "insert_eps_basic"),
        (EPSDilutedFetcher, "insert_eps_diluted"),
        (CashOperatingActivitiesFetcher, "insert_cash_operating_activities"),
        (CashInvestingActivitiesFetcher, "insert_cash_investing_activities"),
        (CashFinancingActivitiesFetcher, "insert_cash_financing_activities"),
        (CurrentAssetsFetcher, "insert_current_assets"),
        (CurrentLiabilitiesFetcher, "insert_current_liabilities"),
        (GrossProfitFetcher, "insert_gross_profit"),
        (OperatingIncomeFetcher, "insert_operating_income"),
        (RetainedEarningsFetcher, "insert_retained_earnings"),
        (CommonStockFetcher, "insert_common_stock"),
        (PreferredStockFetcher, "insert_preferred_stock"),
        (InterestExpenseFetcher, "insert_interest_expense"),
        (DepreciationAndAmortizationFetcher, "insert_depreciation_and_amortization"),
        (CostOfRevenueFetcher, "insert_cost_of_revenue"),
        (OperatingExpensesFetcher, "insert_operating_expenses"),
        (InventoryFetcher, "insert_inventory"),
        (PropertyPlantAndEquipmentFetcher, "insert_property_plant_and_equipment"),
        (GoodwillFetcher, "insert_goodwill"),
        (IntangibleAssetsFetcher, "insert_intangible_assets"),
        (TotalStockholdersEquityFetcher, "insert_total_stockholders_equity"),
        (ResearchAndDevelopmentExpenseFetcher, "insert_research_and_development_expense")
    ]

    # Loop through each period and each fetcher class
    for period in periods:
        for fetcher_class, insert_method in fetchers:
            fetcher_instance = fetcher_class(period=period)
            await getattr(fetcher_instance, insert_method)()

    # Close the database connection after all fetchers have run
    await db_connector.close()


if __name__ == "__main__":
    asyncio.run(fetcher())
