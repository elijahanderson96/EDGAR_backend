import asyncio
import requests
import logging
import io
import pandas as pd
import numpy as np

from database.async_database import db_connector
from database.database import db_connector as normal_connector


class SubmissionsMetadata:
    """
    Class to fetch and store filing data from the SEC for a specific CIK.
    """

    BASE_URL = "https://data.sec.gov/submissions/CIK{cik}.json"

    def __init__(self, cik: str):
        """
        Initialize the SubmissionsMetadata object with a given CIK.

        Args:
            cik (str): Central Index Key (CIK) for the company.
        """
        self.cik = cik.zfill(10)  # Pad CIK to 10 digits
        # XBRL data is the only data within our company facts table. Therefore, if we create a list
        # of unique accession numbers from the submission endpoints, we can reference a list of
        # unique accn numbers within our database and see what data we're missing.
        self.xbrl_accns = None

    def get_filings(self):
        """
        Fetch data from the SEC endpoint and store it in class attributes.
        """
        url = self.BASE_URL.format(cik=self.cik)

        try:
            response = requests.get(url, headers={"User-Agent": "YourAppName/1.0 (your_email@example.com)"})
            response.raise_for_status()  # Raise HTTPError for bad responses

            data = response.json()
            return self._process_data(data)

        except requests.RequestException as e:
            logging.error(f"Error fetching data: {e}")

    def _process_data(self, data):
        """
        Process the fetched JSON data and extract relevant filing mappings.

        Args:
            data (dict): JSON data from the SEC.
        """
        filings = data.get("filings", {}).get("recent", {})

        if not filings:
            logging.warning("No filings found in the response.")
            return

        xbrl_indices = [i for i, v in enumerate(filings['isXBRL']) if v]
        self.xbrl_accns = [filings['accessionNumber'][i] for i in xbrl_indices]
        return self.xbrl_accns


class DatabaseData:
    def __init__(self, cik: str, accns: set):
        self.cik = cik.zfill(10)
        self.accns = accns
        self.data = None

    async def cross_reference_accns(self) -> set:
        q = f'''SELECT cf.accn as accn FROM financials.company_facts cf
                LEFT JOIN metadata.symbols s ON s.symbol_id = cf.symbol_id
                WHERE s.cik=$1'''

        self.data = await db_connector.run_query(q, params=[self.cik])

        return set(self.data['accn'])


class DataRefresher:
    """
    Class to fetch and parse company facts data from the SEC API for a specific CIK and a set of accns.
    """

    BASE_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

    def __init__(self, cik: str):
        """
        Initialize the CompanyFacts object with a given CIK.

        Args:
            cik (str): Central Index Key (CIK) for the company.
        """
        self.cik = cik.zfill(10)  # Pad CIK to 10 digits
        self.data = None

    def fetch_facts(self):
        """
        Fetch data from the SEC endpoint and store it in the class attribute.
        """
        url = self.BASE_URL.format(cik=self.cik)
        try:
            response = requests.get(url, headers={"User-Agent": "YourAppName/1.0 (your_email@example.com)"})
            response.raise_for_status()  # Raise HTTPError for bad responses
            self.data = response.json()
        except requests.RequestException as e:
            logging.error(f"Error fetching data: {e}")
            self.data = None

    def get_facts_by_accns(self, accns: set):
        """
        Extract facts for the given set of accession numbers.

        Args:
            accns (set): A set of accession numbers to filter facts by.

        Returns:
            dict: Facts filtered by the accession numbers.
        """
        if not self.data:
            logging.warning("Data is not loaded. Fetching facts first.")
            self.fetch_facts()

        if not self.data:
            logging.error("Failed to load data. Cannot process facts.")
            return {}

        filtered_facts = {}

        # Iterate over the facts to filter by accns
        for taxonomy, concepts in self.data.get("facts", {}).items():
            for concept_name, concept_data in concepts.items():
                units = concept_data.get("units", {})
                for unit, instances in units.items():
                    for instance in instances:
                        if instance.get("accn") in accns:  # Match against the set of accns
                            if concept_name not in filtered_facts:
                                filtered_facts[concept_name] = {
                                    "label": concept_data.get("label"),
                                    "description": concept_data.get("description"),
                                    "instances": []
                                }
                            # Append the instance data
                            filtered_facts[concept_name]["instances"].append({
                                "unit": unit,
                                "fy": instance.get("fy"),
                                "fp": instance.get("fp"),
                                "value": instance.get("val"),
                                "start_date": instance.get("start"),
                                "end_date": instance.get("end"),
                                "form": instance.get("form"),
                                "filed": instance.get("filed"),
                                "accn": instance.get("accn"),
                            })

        return filtered_facts

    def format_facts_to_dataframe(self, facts: dict) -> pd.DataFrame:
        """Format the facts into a DataFrame similar to process_json_files_optimized."""
        records = []
        for concept_name, concept_data in facts.items():
            for instance in concept_data["instances"]:
                record = {
                    "fact_name": concept_name,
                    "unit": instance["unit"],
                    "fiscal_year": instance["fy"],
                    "fiscal_period": instance["fp"],
                    "value": instance["value"],
                    "start_date": instance["start_date"],
                    "end_date": instance["end_date"],
                    "form": instance["form"],
                    "filed": instance["filed"],
                    "accn": instance["accn"]
                }
                records.append(record)

        df = pd.DataFrame(records)
        df['cik'] = self.cik
        return self.transform_dataframe(df)

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform the DataFrame with required merging and cleaning logic."""
        symbols_df = normal_connector.run_query("SELECT symbol_id, cik FROM metadata.symbols", return_df=True)
        dates_df = normal_connector.run_query("SELECT date_id, date FROM metadata.dates", return_df=True)

        required_columns = ['start_date', 'end_date', 'filed']
        for col in required_columns:
            if col not in df.columns:
                df[col] = None

        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
        df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
        df['filed'] = pd.to_datetime(df['filed'], errors='coerce')

        symbols_df['cik'] = symbols_df['cik'].astype(str)
        dates_df['date'] = pd.to_datetime(dates_df['date'])

        df_merged_symbols = df.merge(symbols_df, on='cik', how='left')

        df_merged_start_date = df_merged_symbols.merge(
            dates_df.rename(columns={'date': 'start_date'}), on='start_date', how='left'
        ).rename(columns={'date_id': 'start_date_id'})

        df_merged_end_date = df_merged_start_date.merge(
            dates_df.rename(columns={'date': 'end_date'}), on='end_date', how='left'
        ).rename(columns={'date_id': 'end_date_id'})

        df_merged_filed_date = df_merged_end_date.merge(
            dates_df.rename(columns={'date': 'filed'}), on='filed', how='left'
        ).rename(columns={'date_id': 'filed_date_id'})

        df = df_merged_filed_date.rename(columns={
            'fy': 'fiscal_year',
            'fp': 'fiscal_period',
            'form': 'form',
            'val': 'value',
            'accn': 'accn'
        })

        df = df[['symbol_id', 'fact_name', 'unit', 'start_date_id', 'end_date_id', 'filed_date_id',
                 'fiscal_year', 'fiscal_period', 'form', 'value', 'accn']]

        df = df.dropna(subset=['symbol_id'])
        df = df.replace({np.nan: None})
        return df

    async def insert_dataframe_to_db(self, df: pd.DataFrame):
        """Insert a dataframe into the database using copy_to_table."""
        output = io.BytesIO()
        df.to_csv(output, sep='\t', index=False, header=False, na_rep='\\N', encoding='utf-8')  # PostgreSQL expects '\\N' for NULLs
        output.seek(0)

        async with db_connector.pool.acquire() as connection:
            try:
                logging.info("Starting data insertion using copy_to_table...")
                await connection.copy_to_table(
                    'company_facts',
                    schema_name='financials',
                    source=output,
                    format='csv',
                    delimiter='\t',
                    columns=df.columns.tolist()
                )
                logging.info(f"Successfully inserted {len(df)} records.")
            except Exception as e:
                logging.error(f"Error during copy_to_table operation: {e}")
                raise


# Async-compatible main method
async def main():
    cik = "0000006845"  # Example CIK
    metadata = SubmissionsMetadata(cik)
    xbrl_filings = set(metadata.get_filings())

    await db_connector.initialize()
    db_data_analyzer = DatabaseData(cik, xbrl_filings)
    db_data = await db_data_analyzer.cross_reference_accns()

    missing_accns = xbrl_filings - db_data
    data_getter = DataRefresher(cik)
    data = data_getter.get_facts_by_accns(missing_accns)
    formatted_data = data_getter.format_facts_to_dataframe(data)
    await data_getter.insert_dataframe_to_db(formatted_data)
    await db_connector.close()
    return missing_accns, len(xbrl_filings), len(db_data), data, formatted_data


# Entry point
if __name__ == "__main__":
    missing_accns = asyncio.run(main())
