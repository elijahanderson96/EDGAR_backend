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
        # Represents the xbrl data that does not currently exist in the database and needs to be fetched.
        # return set(self.accns) - set(self.data['accn'])


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
    return missing_accns, len(xbrl_filings), len(db_data)


# Entry point
if __name__ == "__main__":
    missing_accns = asyncio.run(main())
