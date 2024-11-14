import pandas as pd
import requests
from database.database import db_connector


class SECDataFetcher:
    BASE_URL = "https://data.sec.gov/api/xbrl/frames"
    HEADERS = {
        "User-Agent": "Elijah Anderson (elijahanderson96@gmail.com)"
    }

    def __init__(self, tag: str, currency: str = "USD", period: str = "CY2023Q1"):
        self.tag = tag
        self.currency = currency
        self.period = period

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
        df['cik'] = df['cik'].astype(str).str.zfill(10)  # Ensure CIK is 10 characters with leading zeros
        return df

    def insert_into_db(self, table_name: str):
        try:
            df = self.get_results()
            if df.empty:
                print(f"No data available for insertion into {table_name}.")
                return

            # Rename columns to match schema (symbol_id will be resolved from the CIK)
            df = df.rename(columns={"start": "start_date", "end": "end_date", "val": "value", "accn": "accn"})

            # Determine if this is an instant data frame (only has end_date)
            is_instant_data = "start_date" not in df.columns or df["start_date"].isnull().all()

            # Filter columns based on the type of data (instant or duration)
            if is_instant_data:
                df = df[['cik', 'accn', 'end_date', 'value']]
            else:
                df = df[['cik', 'accn', 'start_date', 'end_date', 'value']]

            # Convert date columns to date format
            if "start_date" in df.columns:
                df['start_date'] = pd.to_datetime(df['start_date']).dt.date
            df['end_date'] = pd.to_datetime(df['end_date']).dt.date

            # Resolve symbol_id based on CIK
            symbol_map = db_connector.run_query("SELECT cik, symbol_id FROM metadata.symbols")
            cik_to_symbol_id = dict(zip(symbol_map['cik'], symbol_map['symbol_id']))
            df['symbol_id'] = df['cik'].map(cik_to_symbol_id)
            df = df.dropna(subset=['symbol_id'])
            df['symbol_id'] = df['symbol_id'].astype(int)

            # Resolve date_ids for start_date and end_date
            date_map = db_connector.run_query("SELECT date_id, date FROM metadata.dates")
            date_to_date_id = dict(zip(pd.to_datetime(date_map['date']).dt.date, date_map['date_id']))

            # Map date IDs based on whether data is instant or duration
            df['start_date_id'] = df['start_date'].map(date_to_date_id) if not is_instant_data else None
            df['end_date_id'] = df['end_date'].map(date_to_date_id)

            # Drop rows where `end_date_id` could not be resolved (it's required for both instant and duration data)
            df = df.dropna(subset=['end_date_id'])

            # Insert data into the specified table
            columns_to_insert = ['symbol_id', 'accn', 'start_date_id', 'end_date_id',
                                 'value'] if not is_instant_data else [
                'symbol_id', 'accn', 'end_date_id', 'value']

            db_connector.insert_dataframe(
                df[columns_to_insert],
                name=table_name,
                schema='financials',
                if_exists='append'
            )
            print(f"Inserted data into {table_name}")
        except Exception as e:
            print(f"Error inserting data into {table_name}: {e}")  # Log the error without halting execution


# Define fetcher classes for specific data types
class RevenueFetcher(SECDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/Revenues", currency=currency, period=period)

    def insert_revenue(self):
        self.insert_into_db("revenue")


class AssetsFetcher(SECDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1I"):
        super().__init__(tag="us-gaap/Assets", currency=currency, period=period)

    def insert_assets(self):
        self.insert_into_db("assets")


class LiabilitiesFetcher(SECDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1I"):
        super().__init__(tag="us-gaap/Liabilities", currency=currency, period=period)

    def insert_liabilities(self):
        self.insert_into_db("liabilities")


class EarningsFetcher(SECDataFetcher):
    def __init__(self, currency: str = "USD", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/NetIncomeLoss", currency=currency, period=period)

    def insert_earnings(self):
        self.insert_into_db("net_income_loss")


class EPSBasicFetcher(SECDataFetcher):
    def __init__(self, currency: str = "USD-per-shares", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/EarningsPerShareBasic", currency=currency, period=period)

    def insert_eps_basic(self):
        self.insert_into_db("eps_basic")


class EPSDilutedFetcher(SECDataFetcher):
    def __init__(self, currency: str = "USD-per-shares", period: str = "CY2023Q1"):
        super().__init__(tag="us-gaap/EarningsPerShareDiluted", currency=currency, period=period)

    def insert_eps_diluted(self):
        self.insert_into_db("eps_diluted")


# Usage example
if __name__ == "__main__":
    from datetime import datetime

    # Define the start year and the list of quarters
    start_year = 2010
    quarters = ["Q1", "Q2", "Q3", "Q4"]
    current_year = datetime.now().year
    current_quarter = (datetime.now().month - 1) // 3 + 1

    fetchers = [
        (RevenueFetcher, "insert_revenue", "CY{}{}"),
        (AssetsFetcher, "insert_assets", "CY{}{}I"),
        (LiabilitiesFetcher, "insert_liabilities", "CY{}{}I"),
        (EarningsFetcher, "insert_earnings", "CY{}{}"),
        (EPSBasicFetcher, "insert_eps_basic", "CY{}{}"),
        (EPSDilutedFetcher, "insert_eps_diluted", "CY{}{}")
    ]

    # Loop through each year and quarter up to the present
    for year in range(start_year, current_year + 1):
        for quarter in quarters:
            if year == current_year and int(quarter[1]) > current_quarter:
                break
            print(f"Fetching data for period: CY{year}{quarter}")
            for fetcher_class, insert_method, period_format in fetchers:
                try:
                    formatted_period = period_format.format(year, quarter)
                    fetcher = fetcher_class(period=formatted_period)
                    getattr(fetcher, insert_method)()
                except Exception as e:
                    print(f"Error processing {fetcher_class.__name__} for period {formatted_period}: {e}")
