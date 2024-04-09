import requests


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

        return cik_mapping

    except requests.exceptions.RequestException as e:
        print(f"Error retrieving CIK to symbol mapping: {str(e)}")
        return None


symbols = resolve_cik_to_symbol_mapping()
