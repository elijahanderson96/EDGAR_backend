import time

import requests
import pandas as pd

from database.database import db_connector
from edgar.symbols import resolve_cik_to_symbol_mapping


class FinancialStatementGatherer:
    def __init__(self, cik):
        self.cik = cik
        self.headers = {
            "User-Agent": 'My Data Science Project (elijahanderson96@gmail.com)',
            "Accept-Encoding": "gzip, deflate",
        }
        self.url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{self.cik}.json"
        self.data = None

    def get_company_facts(self):
        try:
            response = requests.get(self.url, headers=self.headers)
            response.raise_for_status()
            self.data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error retrieving company facts for CIK {self.cik}: {str(e)}")
            self.data = None

    def parse_company_facts(self):
        if self.data:
            us_gaap_facts = self.data['facts']['us-gaap']

            data_list = []
            for fact_key, fact_data in us_gaap_facts.items():
                label = fact_data.get('label', '')
                description = fact_data.get('description', '')

                if 'units' in fact_data and 'USD' in fact_data['units']:
                    usd_data_list = fact_data['units']['USD']
                    for usd_data in usd_data_list:
                        row_data = {
                            'fact_key': fact_key,
                            'label': label,
                            'description': description,
                            'accession_number': usd_data.get('accn', ''),
                            'end_date': usd_data.get('end', ''),
                            'filed_date': usd_data.get('filed', ''),
                            'form': usd_data.get('form', ''),
                            'fiscal_period': usd_data.get('fp', ''),
                            'fiscal_year': usd_data.get('fy', ''),
                            'value': usd_data.get('val', ''),
                            'frame': usd_data.get('frame', '')
                        }
                        data_list.append(row_data)

            df = pd.DataFrame(data_list)
            return df
        else:
            print("No company facts data available.")
            return None


# cik = "0000320193"  # CIK number for Apple Inc.
cik_to_symbol_mapping = resolve_cik_to_symbol_mapping()

for cik, company_info in cik_to_symbol_mapping.items():
    try:
        symbol = company_info["symbol"]
        table_name = symbol.lower().replace("-", "_")

        gatherer = FinancialStatementGatherer(cik)
        gatherer.get_company_facts()
        facts = gatherer.parse_company_facts()

        if facts is not None:
            db_connector.insert_dataframe(facts, name=table_name,
                                          schema='company_facts', if_exists='replace', index=False)
            print(f"Inserted data for: {symbol}")
            print(facts.head(10))
        else:
            print(f"No data available for: {symbol}")

        time.sleep(1)

    except Exception as e:
        print(f"An error occurred while processing {symbol}: {str(e)}")
        continue

