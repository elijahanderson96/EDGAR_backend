import pandas as pd
import requests
from database.database import db_connector


class CompanySharesFetcher:
    def __init__(self, cik: str):
        # Ensure the CIK is zero-padded to 10 digits
        self.cik = cik.zfill(10)
        self.base_url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{self.cik}.json"
        self.headers = {
            "User-Agent": "Elijah Anderson (elijahanderson96@gmail.com)"
        }

    def fetch_data(self):
        # Make the GET request
        response = requests.get(self.base_url, headers=self.headers)

        # Check if the request was successful
        if response.status_code == 200:
            data = response.json()
            # Extract the shares outstanding data
            shares_data = pd.DataFrame(self._parse_shares_outstanding(data))
            return shares_data
        else:
            print(f"Failed to retrieve data for CIK {self.cik}. Status code: {response.status_code}")
            return pd.DataFrame()  # Return empty DataFrame if request fails

    def _parse_shares_outstanding(self, data):
        # Extract shares outstanding information
        shares_outstanding = []
        try:
            shares_facts = data['facts']['dei']['EntityCommonStockSharesOutstanding']
            for fact in shares_facts['units']['shares']:
                shares_outstanding.append({
                    "end_date": fact.get("end"),
                    "value": fact.get("val"),
                    "frame": fact.get("frame")
                })
        except KeyError:
            print(f"Shares outstanding data not found for CIK {self.cik}.")
        return shares_outstanding


def fetch_all_shares_data():
    # Fetch all CIKs and symbols from the database
    symbols_df = db_connector.run_query("SELECT symbol, cik FROM metadata.symbols")

    if symbols_df.empty:
        print("No symbols found in the database.")
        return

    all_shares_data = []  # Initialize an empty list to store all shares data

    for _, row in symbols_df[51:].iterrows():
        cik = row['cik']
        symbol = row['symbol']

        fetcher = CompanySharesFetcher(cik)
        shares_data = fetcher.fetch_data()

        if not shares_data.empty:
            # Add CIK and symbol columns to each DataFrame
            shares_data['cik'] = cik
            shares_data['symbol'] = symbol
            # Append to the cumulative list
            all_shares_data.append(shares_data)
        else:
            print(f"No shares outstanding data found for symbol {symbol} with CIK {cik}.")
    # Concatenate all shares data into a single DataFrame if there is data
    if all_shares_data:
        all_shares_data = pd.concat(all_shares_data, ignore_index=True)

        # Convert dates to proper format
        all_shares_data['end_date'] = pd.to_datetime(all_shares_data['end_date']).dt.date
        all_shares_data['value'] = pd.to_numeric(all_shares_data['value'], errors='coerce')  # Ensure values are numeric

        # Resolve symbol_id based on symbol
        symbol_map = db_connector.run_query("SELECT symbol, symbol_id FROM metadata.symbols")
        symbol_to_id = dict(zip(symbol_map['symbol'], symbol_map['symbol_id']))
        all_shares_data['symbol_id'] = all_shares_data['symbol'].map(symbol_to_id)

        # Drop rows where symbol_id could not be resolved
        all_shares_data = all_shares_data.dropna(subset=['symbol_id'])
        all_shares_data['symbol_id'] = all_shares_data['symbol_id'].astype(int)

        # Resolve end_date_id based on the metadata.dates table
        date_map = db_connector.run_query("SELECT date_id, date FROM metadata.dates")
        date_to_date_id = dict(zip(pd.to_datetime(date_map['date']).dt.date, date_map['date_id']))
        all_shares_data['end_date_id'] = all_shares_data['end_date'].map(date_to_date_id)

        # Drop rows where end_date_id could not be resolved
        all_shares_data = all_shares_data.dropna(subset=['end_date_id'])
        all_shares_data['end_date_id'] = all_shares_data['end_date_id'].astype(int)

        # Insert the shares data into the database
        db_connector.insert_dataframe(
            all_shares_data[['symbol_id', 'value', 'end_date_id', 'frame']],
            name='shares',
            schema='financials',
            if_exists='append'
        )
        print("Inserted shares outstanding data into the financials.shares table.")
    else:
        print("No shares outstanding data to insert.")


# Run the fetch process
if __name__ == "__main__":
    fetch_all_shares_data()
