import asyncio
import pandas as pd
import requests
from database.async_database import db_connector  # Async database connector with necessary methods


class SECFramesDataFetcher:
    BASE_URL = "https://data.sec.gov/api/xbrl/companyconcept"
    HEADERS = {
        "User-Agent": "Elijah Anderson (elijahanderson96@gmail.com)"
    }

    def __init__(self, cik: str, endpoint: str):
        self.cik = cik
        self.endpoint = endpoint
        self.taxonomy = None
        self.tag = None
        self.label = None
        self.description = None
        self.entity_name = None
        self.units_data = {}  # Dictionary to hold data by unit type
        self.data_df = pd.DataFrame()  # Placeholder for resolved DataFrame

    def _construct_url(self) -> str:
        return f"{self.BASE_URL}/CIK{self.cik}/us-gaap/{self.endpoint}.json"

    def fetch_data(self) -> dict:
        url = self._construct_url()
        print(f"Fetching data from URL: {url}")
        response = requests.get(url, headers=self.HEADERS)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            return {}

    def unpack_data(self, data: dict):
        """Unpacks JSON response and sets class attributes for metadata and units."""
        self.taxonomy = data.get('taxonomy')
        self.tag = data.get('tag')
        self.label = data.get('label')
        self.description = data.get('description')
        self.entity_name = data.get('entityName')
        self.units_data = data.get('units', {})

        if not self.units_data:
            print("No data available in 'units' field.")
        else:
            print(f"Data available in units: {', '.join(self.units_data.keys())}")

    async def resolve_metadata(self):
        """Fetches symbol_id and date_id mappings to resolve necessary identifiers."""
        # Map CIK to symbol_id
        symbol_map = await db_connector.run_query("SELECT cik, symbol_id FROM metadata.symbols")
        cik_to_symbol_id = dict(zip(symbol_map['cik'], symbol_map['symbol_id']))
        self.symbol_id = cik_to_symbol_id.get(self.cik)

        # Map dates to date_id
        date_map = await db_connector.run_query("SELECT date_id, date FROM metadata.dates")
        self.date_to_date_id = dict(zip(pd.to_datetime(date_map['date']).dt.date, date_map['date_id']))

        if not self.symbol_id:
            raise ValueError(f"Symbol ID not found for CIK: {self.cik}")

    async def get_data(self):
        """Fetches and processes data, then converts it to a DataFrame with resolved IDs."""
        # Fetch and unpack data
        data = self.fetch_data()
        self.unpack_data(data)

        if not self.units_data:
            print("No data available in 'units' field.")
            return pd.DataFrame()

        # Process each unit type
        dfs = []
        for unit, records in self.units_data.items():
            df = pd.DataFrame(records)
            df['cik'] = self.cik
            df['symbol_id'] = self.symbol_id
            df['unit'] = unit  # Add a column to specify the unit type

            # Resolve date IDs
            df['start_date_id'] = pd.to_datetime(df['start']).map(
                self.date_to_date_id) if 'start' in df.columns else None
            df['end_date_id'] = pd.to_datetime(df['end']).map(self.date_to_date_id)
            df['filed_date_id'] = pd.to_datetime(df['filed']).map(self.date_to_date_id)

            # Drop rows without resolved end_date_id (mandatory field)
            df = df.dropna(subset=['end_date_id'])

            dfs.append(df)

        # Concatenate all DataFrames for different units
        if dfs:
            self.data_df = pd.concat(dfs, ignore_index=True)
        else:
            print("No valid data to process.")

        # Select relevant columns
        self.data_df = self.data_df[
            ['symbol_id', 'start_date_id', 'end_date_id', 'filed_date_id', 'fy', 'fp', 'form', 'val', 'accn', 'unit']]
        self.data_df = self.data_df.rename(columns={"val": "value"})

        return self.data_df

    async def save_to_db(self, table_name: str):
        """Inserts data into the database, avoiding duplicate entries."""
        if self.data_df.empty:
            print("No data available to save.")
            return

        # Check for duplicates
        is_instant_data = ("start_date_id" not in self.data_df.columns.tolist()
                           or self.data_df["start_date_id"].isnull().all())

        unique_columns = ['symbol_id', 'end_date_id'] if is_instant_data else ['symbol_id', 'start_date_id',
                                                                               'end_date_id']

        df_filtered = await db_connector.drop_existing_rows(
            self.data_df, f"financials.{table_name}", unique_key_columns=unique_columns
        )

        if df_filtered.empty:
            print(f"All rows already exist in {table_name}. No new data to insert.")
            return

        df_filtered.rename(inplace=True, columns={'fy': 'fiscal_year', 'fp': 'fiscal_period'})

        # Insert new records
        columns_to_insert = ['symbol_id', 'start_date_id', 'end_date_id', 'filed_date_id', 'fiscal_year',
                             'fiscal_period', 'form', 'value', 'accn']

        if is_instant_data:
            columns_to_insert.remove('start_date_id')

        records = df_filtered[columns_to_insert].values.tolist()

        insert_query = f"""
            INSERT INTO financials.{table_name} ({', '.join(columns_to_insert)})
            VALUES ({', '.join([f'${i + 1}' for i in range(len(columns_to_insert))])})
        """

        await db_connector.pool.executemany(insert_query, records)
        print(f"Inserted new data into {table_name}")

    def display_metadata(self):
        """Displays unpacked metadata information."""
        # print(f"Entity Name: {self.entity_name}")
        # print(f"CIK: {self.cik}")
        # print(f"Concept Tag: {self.tag}")
        # print(f"Label: {self.label}")
        # print(f"Description: {self.description}")
        # if self.units_data:
        #     print(f"Available units: {', '.join(self.units_data.keys())}")
        pass


# Example usage
async def main():
    await db_connector.initialize()  # Initialize the DB connection pool

    endpoint_to_table_name = {
        "Assets": "assets",
        "NetCashProvidedByUsedInFinancingActivities": "cash_financing_activities",
        "NetCashProvidedByUsedInInvestingActivities": "cash_investing_activities",
        "NetCashProvidedByUsedInOperatingActivities": "cash_operating_activities",
        "CommonStockSharesOutstanding": "common_stock",
        "ComprehensiveIncomeNetOfTax": "comprehensive_income",
        "CostOfRevenue": "cost_of_revenue",
        "AssetsCurrent": "current_assets",
        "LiabilitiesCurrent": "current_liabilities",
        "DepreciationAndAmortization": "depreciation_and_amortization",
        "EarningsPerShareBasic": "eps_basic",
        "EarningsPerShareDiluted": "eps_diluted",
        "Goodwill": "goodwill",
        "GrossProfit": "gross_profit",
        "IntangibleAssetsNetExcludingGoodwill": "intangible_assets",
        "InterestExpense": "interest_expense",
        "InventoryNet": "inventory",
        "Liabilities": "liabilities",
        "NetIncomeLoss": "net_income_loss",
        "OperatingExpenses": "operating_expenses",
        "OperatingIncomeLoss": "operating_income_loss",
        "PreferredStockValue": "preferred_stock",
        "PropertyPlantAndEquipmentNet": "property_plant_and_equipment",
        "ResearchAndDevelopmentExpense": "research_and_development_expense",
        "RetainedEarningsAccumulatedDeficit": "retained_earnings",
        "Revenues": "revenue",
        # "WeightedAverageNumberOfDilutedSharesOutstanding": "shares",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": "total_stockholders_equity"
    }

    # Fetch all CIK numbers from the metadata table
    cik_symbols = await db_connector.run_query("SELECT cik FROM metadata.symbols")
    ciks = cik_symbols['cik'].unique()

    # Loop through each CIK and endpoint, fetch and save data
    for cik in ciks:
        for endpoint, table_name in endpoint_to_table_name.items():
            try:
                fetcher = SECFramesDataFetcher(cik=str(cik).zfill(10), endpoint=endpoint)
                await fetcher.resolve_metadata()
                data_df = await fetcher.get_data()
                fetcher.display_metadata()
                await fetcher.save_to_db(table_name=table_name)
            except Exception as e:
                print(e)

    await db_connector.close()

asyncio.run(main())
