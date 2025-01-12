import asyncio
import requests
import logging

from database.async_database import db_connector


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


# Async-compatible main method
async def main():
    cik = "0000006845"  # Example CIK
    metadata = SubmissionsMetadata(cik)
    xbrl_filings = set(metadata.get_filings())

    await db_connector.initialize()
    db_data_analyzer = DatabaseData(cik, xbrl_filings)
    db_data = await db_data_analyzer.cross_reference_accns()
    await db_connector.close()

    missing_accns = xbrl_filings - db_data
    data_getter = DataRefresher(cik)
    data = data_getter.get_facts_by_accns(missing_accns)

    return missing_accns, len(xbrl_filings), len(db_data), data


# Entry point
if __name__ == "__main__":
    missing_accns = asyncio.run(main())
