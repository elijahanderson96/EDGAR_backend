import asyncio

import numpy as np
import requests
from database.async_database import db_connector  # Async database connector with necessary methods

import pandas as pd

# Set maximum number of rows and columns to display
pd.set_option('display.max_rows', 100)  # Set to None to display all rows
pd.set_option('display.max_columns', 100)  # Set to None to display all columns


class SECCompanyFactsFetcher:
    BASE_URL = "https://data.sec.gov/api/xbrl/companyfacts"
    HEADERS = {
        "User-Agent": "Elijah Anderson (elijahanderson96@gmail.com)"
    }

    def __init__(self, cik: str):
        self.cik = cik
        self.entity_name = None
        self.symbol_id = None
        self.date_to_date_id = {}
        self.aggregated_data = []  # List to hold data for all facts

    def _construct_url(self) -> str:
        return f"{self.BASE_URL}/CIK{self.cik}.json"

    def fetch_data(self) -> dict:
        url = self._construct_url()
        print(f"Fetching data from URL: {url}")
        response = requests.get(url, headers=self.HEADERS)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
            return {}

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

    async def process_fact(self, fact_name: str, fact_details: dict):
        """Processes and appends each unit type for the given fact name to aggregated data."""
        for unit, records in fact_details.get('units', {}).items():
            df = pd.DataFrame(records)

            # Rename columns for consistency with database schema
            df.rename(inplace=True, columns={'val': 'value', 'fp': 'fiscal_period', 'fy': 'fiscal_year'})

            # Determine if data is instantaneous or interval based on presence of 'start' column
            is_instantaneous = 'start' not in df.columns

            # Add essential metadata to DataFrame
            df['cik'] = self.cik
            df['symbol_id'] = self.symbol_id
            df['fact_name'] = fact_name

            # Resolve date IDs
            df['start_date_id'] = pd.to_datetime(df['start']).map(
                self.date_to_date_id) if not is_instantaneous else None
            df['end_date_id'] = pd.to_datetime(df['end']).map(self.date_to_date_id)
            df['filed_date_id'] = pd.to_datetime(df['filed']).map(self.date_to_date_id)

            # Drop rows without resolved end_date_id (mandatory field)
            df = df.dropna(subset=['end_date_id'])

            # Select relevant columns for insertion
            required_columns = ['symbol_id', 'fact_name', 'end_date_id', 'filed_date_id', 'fiscal_year',
                                'fiscal_period', 'form', 'value', 'accn']
            if not is_instantaneous:
                required_columns.insert(2, 'start_date_id')

            # Validate that the required columns exist in the DataFrame
            missing_columns = set(required_columns) - set(df.columns)
            if missing_columns:
                print(f"Warning: Missing columns in {fact_name} data for unit '{unit}': {missing_columns}")
                continue

            # Ensure only necessary columns are in the DataFrame
            df = df[required_columns]

            # Append the DataFrame to the list of aggregated data
            self.aggregated_data.append(df)

    async def save_aggregated_data_to_db(self):
        """Inserts all aggregated data into the company_facts table in one bulk operation."""
        if not self.aggregated_data:
            print("No data available to save.")
            return

        # Concatenate all fact DataFrames for the CIK
        aggregated_df = pd.concat(self.aggregated_data, ignore_index=True)

        unique_columns = ['symbol_id', 'fact_name', 'end_date_id', 'filed_date_id', 'fiscal_year',
                          'fiscal_period', 'form', 'value', 'accn', 'start_date_id']

        aggregated_df = aggregated_df.replace(np.nan, None)
        # Drop rows without resolved end_date_id (mandatory field)
        aggregated_df = aggregated_df.dropna(subset=['end_date_id'])
        print(aggregated_df.shape)
        df_filtered = aggregated_df
        # df_filtered = await db_connector.drop_existing_rows(aggregated_df, "financials.company_facts",
        #                                                    unique_key_columns=unique_columns)
        print(df_filtered.shape)
        if df_filtered.empty:
            print("All rows already exist in financials.company_facts. No new data to insert.")
            return

        # Prepare and execute the bulk insert
        columns_to_insert = list(df_filtered.columns)
        records = df_filtered[columns_to_insert].values.tolist()
        insert_query = f"""
            INSERT INTO financials.company_facts ({', '.join(columns_to_insert)})
            VALUES ({', '.join([f'${i + 1}' for i in range(len(columns_to_insert))])})
        """
        await db_connector.pool.executemany(insert_query, records)
        print("Inserted new data into financials.company_facts")

    async def fetch_and_save_all_facts(self):
        """Main function to fetch all facts for the CIK, aggregate them, and save to the unified table."""
        await self.resolve_metadata()

        data = self.fetch_data()
        if 'facts' not in data:
            print(f"No facts data found for CIK: {self.cik}")
            return

        self.entity_name = data.get('entityName')
        facts = data['facts']

        # Process each fact and append to aggregated data list
        for taxonomy, fact_dict in facts.items():
            for fact_name, fact_details in fact_dict.items():
                try:
                    await self.process_fact(fact_name, fact_details)
                except Exception as e:
                    print(f"Error processing fact {fact_name} for CIK {self.cik}: {e}")
        # Bulk insert all aggregated data for the CIK
        await self.save_aggregated_data_to_db()


# Example usage
async def main():
    await db_connector.initialize()  # Initialize the DB connection pool

    # cik_symbols = await db_connector.run_query("SELECT cik FROM metadata.symbols")
    # ciks = cik_symbols['cik'].unique()
    #
    # # Fetch distinct CIKs already present in the database
    # ciks_already_present = await db_connector.run_query('''
    #     SELECT DISTINCT(symbols.cik)
    #     FROM financials.company_facts
    #     LEFT JOIN metadata.symbols ON company_facts.symbol_id = symbols.symbol_id;
    # ''')
    #
    # # Convert to sets to perform the subtraction operation
    # ciks = list(set(ciks) - set(ciks_already_present['cik']))

    # for cik in ciks:
    cik = '0001481513'
    try:
        fetcher = SECCompanyFactsFetcher(cik=str(cik).zfill(10))
        await fetcher.fetch_and_save_all_facts()
    except Exception as e:
        print(f"Error processing CIK {cik}: {e}")

    await db_connector.close()


# Run the main function
asyncio.run(main())
