import logging
import time

import requests
import pandas as pd
from tqdm import tqdm

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
        self.xbrl_accns = None

    def get_filings(self):
        """
        Fetch data from the SEC endpoint and store it in class attributes.
        Implements backoff using the Retry-After header if present.
        """
        url = self.BASE_URL.format(cik=self.cik)
        max_retries = 5

        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers={"User-Agent": "Elijah Anderson (elijahanderson96@gmail.com)"})

                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        wait_time = int(retry_after)
                    else:
                        wait_time = 2 ** attempt  # Default exponential backoff

                    time.sleep(wait_time)
                    continue  # Retry after waiting

                response.raise_for_status()
                data = response.json()
                return self._process_data(data)

            except requests.RequestException as e:
                if attempt == max_retries - 1:
                    raise  # Raise the last exception if all retries fail
        return None  # Should only be reached if max_retries is 0 or loop is bypassed

    def _process_data(self, data):
        """
        Process the fetched JSON data and extract relevant filing mappings.

        Args:
            data (dict): JSON data from the SEC.
        """
        filings = data.get("filings", {}).get("recent", {})

        if not filings:
            return None  # Return None if no filings

        xbrl_indices = [i for i, v in enumerate(filings.get('isXBRL', [])) if v]
        if not xbrl_indices:  # Ensure 'accessionNumber' exists and has items for xbrl_indices
            self.xbrl_accns = []
            return self.xbrl_accns

        all_accns = filings.get('accessionNumber', [])
        self.xbrl_accns = [all_accns[i] for i in xbrl_indices if i < len(all_accns)]
        return self.xbrl_accns


class DatabaseData:
    def __init__(self, cik: str, accns: set):
        self.cik = cik.zfill(10)
        self.accns = accns
        self.data = None

    def cross_reference_accns(self) -> set:
        q = f'''SELECT cf.accn as accn FROM financials.company_facts cf
                LEFT JOIN metadata.symbols s ON s.symbol_id = cf.symbol_id
                WHERE s.cik=%s'''
        query_result = normal_connector.run_query(q, params=[self.cik])

        if isinstance(query_result, pd.DataFrame):
            self.data = query_result
            return set(self.data['accn'])
        elif isinstance(query_result, list) and query_result and isinstance(query_result[0], dict):  # list of dicts
            return set(row['accn'] for row in query_result)
        return set()


class DataRefresher:
    """
    Class to fetch and parse company facts data from the SEC API for a specific CIK and a set of accns.
    """

    BASE_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

    def __init__(self, cik: str):
        """
        Initialize the DataRefresher object with a given CIK.

        Args:
            cik (str): Central Index Key (CIK) for the company.
        """
        self.cik = cik.zfill(10)  # Pad CIK to 10 digits
        self.data = None

    def fetch_facts(self):
        """
        Fetch data from the SEC endpoint and store it in the class attribute.
        Implements backoff using the Retry-After header if present.
        """
        url = self.BASE_URL.format(cik=self.cik)
        max_retries = 5

        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers={"User-Agent": "Elijah Anderson (elijahanderson96@gmail.com)"})

                if response.status_code == 429:
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        wait_time = int(retry_after)
                    else:
                        wait_time = 2 ** attempt

                    time.sleep(wait_time)
                    continue

                response.raise_for_status()
                self.data = response.json()
                return  # Successfully fetched data

            except requests.RequestException as e:
                if attempt == max_retries - 1:
                    self.data = None  # Ensure data is None on final failure
                    raise
        self.data = None  # Should only be reached if max_retries is 0

    def get_facts_by_accns(self, accns: set):
        """
        Extract facts for the given set of accession numbers.

        Args:
            accns (set): A set of accession numbers to filter facts by.

        Returns:
            dict: Facts filtered by the accession numbers.
        """
        self.fetch_facts()

        if not self.data:
            return {}

        filtered_facts = {}
        for taxonomy, concepts in self.data.get("facts", {}).items():
            for concept_name, concept_data in concepts.items():
                units = concept_data.get("units", {})
                for unit, instances in units.items():
                    for instance in instances:
                        if instance.get("accn") in accns:
                            if concept_name not in filtered_facts:
                                filtered_facts[concept_name] = {
                                    "label": concept_data.get("label"),
                                    "description": concept_data.get("description"),
                                    "instances": []
                                }
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
        """Format the facts into a DataFrame."""
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

        if not records:
            return pd.DataFrame()  # Return empty DataFrame if no records

        df = pd.DataFrame(records)
        df['cik'] = self.cik
        return self.transform_dataframe(df)

    def transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform the DataFrame with required merging and cleaning logic."""
        if df.empty:
            return df

        symbols_df = normal_connector.run_query("SELECT symbol_id, cik FROM metadata.symbols", return_df=True)
        dates_df = normal_connector.run_query("SELECT date_id, date FROM metadata.dates", return_df=True)

        required_columns = ['start_date', 'end_date', 'filed']
        for col in required_columns:
            if col not in df.columns:
                df[col] = None

        df['start_date'] = pd.to_datetime(df['start_date'], errors='coerce')
        df['end_date'] = pd.to_datetime(df['end_date'], errors='coerce')
        df['filed'] = pd.to_datetime(df['filed'], errors='coerce')

        symbols_df['cik'] = symbols_df['cik'].astype(str).str.zfill(10)  # Ensure CIK format matches for merge
        df['cik'] = df['cik'].astype(str).str.zfill(10)

        dates_df['date'] = pd.to_datetime(dates_df['date'])

        df_merged_symbols = df.merge(symbols_df, on='cik', how='left')

        df_merged_start_date = df_merged_symbols.merge(
            dates_df.rename(columns={'date': 'start_date_dt', 'date_id': 'start_date_id'}),
            left_on='start_date', right_on='start_date_dt', how='left'
        ).drop(columns=['start_date_dt'], errors='ignore')

        df_merged_end_date = df_merged_start_date.merge(
            dates_df.rename(columns={'date': 'end_date_dt', 'date_id': 'end_date_id'}),
            left_on='end_date', right_on='end_date_dt', how='left'
        ).drop(columns=['end_date_dt'], errors='ignore')

        df_merged_filed_date = df_merged_end_date.merge(
            dates_df.rename(columns={'date': 'filed_dt', 'date_id': 'filed_date_id'}),
            left_on='filed', right_on='filed_dt', how='left'
        ).drop(columns=['filed_dt'], errors='ignore')

        df = df_merged_filed_date  # Already renamed in the merge lines

        # Select and rename columns as per the original script's intent
        # The original script renamed columns after merging, this version renames during merge for clarity
        # Ensure fiscal_year, fiscal_period, form, value, accn are present or map them if names differ
        # Original df had: fiscal_year, fiscal_period, value, form, accn from records

        final_columns = ['symbol_id', 'fact_name', 'unit', 'start_date_id', 'end_date_id', 'filed_date_id',
                         'fiscal_year', 'fiscal_period', 'form', 'value', 'accn']

        # Ensure all target columns exist, add if missing (e.g. if original df didn't have them)
        for col in final_columns:
            if col not in df.columns:
                df[col] = None

        df = df[final_columns]

        df = df.dropna(subset=['symbol_id'])
        if df.empty:
            return df

        int_columns = ['symbol_id', 'start_date_id', 'end_date_id', 'filed_date_id', 'fiscal_year']
        for col in int_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        df = df.replace({pd.NA: None})
        return df

    def insert_dataframe_to_db(self, df: pd.DataFrame):
        """Insert a DataFrame into the database."""
        if df.empty:
            return
        if isinstance(df, pd.DataFrame):
            normal_connector.insert_dataframe(df, name='company_facts', schema='financials', if_exists='append')
        return

def get_all_ciks():
    query = """
        SELECT cik, symbol
        FROM metadata.symbols
        ORDER BY cik
    """
    # Ensure CIKs are read as strings and padded if necessary for consistency
    ciks_df = normal_connector.run_query(query, return_df=True)
    if ciks_df.empty:
        return []
    ciks_df['cik'] = ciks_df['cik'].astype(str).str.zfill(10)
    return list(zip(ciks_df['cik'], ciks_df['symbol']))


def process_cik(cik, symbol):
    """Process data for a single CIK."""
    try:
        metadata = SubmissionsMetadata(cik)
        xbrl_filings_list = metadata.get_filings()

        if not xbrl_filings_list:  # Handles None or empty list
            return cik, 0, 0, 0  # CIK, total_xbrl, db_found, inserted_count

        xbrl_filings_set = set(xbrl_filings_list)

        db_data_analyzer = DatabaseData(cik, xbrl_filings_set)
        db_accns_set = db_data_analyzer.cross_reference_accns()

        missing_accns = xbrl_filings_set - db_accns_set

        if not missing_accns:
            return cik, len(xbrl_filings_set), len(db_accns_set), 0

        data_refresher = DataRefresher(cik)
        new_facts_data = data_refresher.get_facts_by_accns(missing_accns)

        inserted_count = 0
        if new_facts_data:
            formatted_df = data_refresher.format_facts_to_dataframe(new_facts_data)
            if not formatted_df.empty:
                data_refresher.insert_dataframe_to_db(formatted_df)
                inserted_count = len(formatted_df)

        return cik, len(xbrl_filings_set), len(db_accns_set), inserted_count

    except Exception as e:
        # Optionally, logger.info or handle the error for this CIK without full logging
        # logger.info(f"Error processing CIK {cik}, Symbol: {symbol}: {e}")
        return cik, 0, 0, 0  # Indicate failure or no data processed for this CIK


def main(logger):
    """Main function to process all CIKs."""
    if hasattr(normal_connector, 'initialize'):
        normal_connector.initialize()

    processed_count = 0
    total_inserted_records = 0

    try:
        cik_symbol_pairs = get_all_ciks()

        if not cik_symbol_pairs:
            logger.info("No CIKs found to process.")
            return

        for cik, symbol in tqdm(cik_symbol_pairs, desc="Processing CIKs"):
            try:
                logger.info("Processing Symbol: {}, CIK: {}".format(symbol,cik))
                _, _, _, inserted = process_cik(cik, symbol)
                if inserted > 0:
                    logger.info('Inserted {} records for {}'.format(inserted, cik))
                    processed_count += 1
                    total_inserted_records += inserted
            except Exception as e:
                # Minimal error reporting for a specific CIK if needed, otherwise it's silent
                logger.info(f"Unhandled error for CIK {cik} ({symbol}): {e}")

        logger.info(f"Processing completed. Inserted data for {processed_count}/{len(cik_symbol_pairs)} CIKs.")
        logger.info(f"Total records inserted: {total_inserted_records}")

    except Exception as e:
        # Minimal error reporting for main process error
        logger.info(f"An error occurred in the main process: {e}")

    finally:
        if hasattr(normal_connector, 'close'):
            normal_connector.close()


if __name__ == "__main__":
    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Basic formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    main(root_logger)
