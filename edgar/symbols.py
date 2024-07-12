import pandas as pd
import requests
from database.database import db_connector


def resolve_cik_to_symbol_mapping():
    headers = {
        "User-Agent": 'My Data Science Project (elijahanderson96@gmail.com)',
        "Accept-Encoding": "gzip, deflate",
    }

    url = "https://www.sec.gov/files/company_tickers.json"

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()

        cik_mapping = {}
        for entry in data.values():
            cik = str(entry['cik_str']).zfill(10)
            symbol = entry['ticker']
            title = entry['title']
            cik_mapping[cik] = {
                'symbol': symbol,
                'title': title
            }

        # Convert the dictionary to a DataFrame
        df = pd.DataFrame.from_dict(cik_mapping, orient='index')
        df.reset_index(inplace=True)
        df.columns = ['cik', 'symbol', 'title']

        #db_connector.insert_dataframe(df, name='cik_mapping', schema='company_facts', if_exists='replace')

        return df

    except requests.exceptions.RequestException as e:
        print(f"Error retrieving CIK to symbol mapping: {str(e)}")
        return None


symbols = resolve_cik_to_symbol_mapping()
