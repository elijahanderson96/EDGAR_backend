import os

import pandas as pd
from bs4 import BeautifulSoup
import argparse
import re
from dateutil import parser as date_parser
from database.database import db_connector

# parser = argparse.ArgumentParser(description="Extract and store XBRL data from an HTML file")
# parser.add_argument("html_path", type=str, help="Path to the HTML file")
# parser.add_argument("symbol", type=str, help="Stock symbol")
# args = parser.parse_args()
#
# with open(args.html_path, 'r', encoding='utf-8') as file:
#     html_content = file.read()

with open(r"C:\Users\Elijah\PycharmProjects\edgar_backend\sec-edgar-filings\AAPL\10-Q\0000320193-23-000064\primary-document.html") as file:
    html_content = file.read()

soup = BeautifulSoup(html_content, 'html.parser')

# Define common XBRL tag prefixes and exclude irrelevant tags
xbrl_prefixes = ["dei", "us-gaap", "ix"]
excluded_tags = {"a"}  # Exclude 'a' tags which are usually hyperlinks
data = []


# Function to parse context data in ix:resources
def parse_contexts(soup):
    contexts = {}
    for context in soup.find_all("xbrli:context"):
        context_id = context.get("id")
        period = context.find("xbrli:period")
        entity = context.find("xbrli:identifier")

        if period:
            if period.find("xbrli:startdate") and period.find("xbrli:enddate"):
                start_date = period.find("xbrli:startdate").text
                end_date = period.find("xbrli:enddate").text
                contexts[context_id] = {"start_date": start_date, "end_date": end_date}
            elif period.find("xbrli:instant"):
                instant = period.find("xbrli:instant").text
                contexts[context_id] = {"instant_date": instant}

        if entity:
            contexts[context_id]["entity"] = entity.get_text(strip=True)
    return contexts


# Extract context data from ix:resources
contexts = parse_contexts(soup)

# Extract relevant XBRL data elements
for element in soup.find_all(True):
    # Check if the element's tag is excluded or lacks necessary attributes
    if element.name in excluded_tags:
        continue  # Skip irrelevant tags

    # Filter for tags that match our XBRL prefixes
    if any(prefix in element.name or element.get("name", "") for prefix in xbrl_prefixes):
        text_value = element.get_text(strip=True)
        scale = element.get("scale", "")
        decimals = element.get("decimals", "")

        # Convert text to float if it's numeric and apply scaling
        try:
            value = float(text_value.replace(",", "")) if text_value else None
            if value is not None:
                if scale:
                    value *= 10 ** int(scale)
                if decimals.isdigit():
                    value = round(value, int(decimals))
            text_value = value
        except ValueError:
            text_value = None  # Keep text as-is if conversion fails

        # Only include elements with meaningful data in the 'text' field or other key attributes
        if text_value is not None or element.get("unitref") or element.get("contextref"):
            element_data = {
                "tag": element.name,
                "name": element.get("name", ""),
                "contextref": element.get("contextref", ""),
                "unitref": element.get("unitref", ""),
                "text": text_value,
                "start_date": None,
                "end_date": None,
                "instant_date": None
            }

            # Attach context date information if available
            context_info = contexts.get(element_data["contextref"], {})
            element_data.update(context_info)
            data.append(element_data)

# Convert data to DataFrame and drop duplicates
df = pd.DataFrame(data)
print(df)
df['context_details'] = df['contextref'].map(contexts)
df['start_date'] = df['context_details'].apply(lambda x: x.get('start_date') if isinstance(x, dict) else None)
df['end_date'] = df['context_details'].apply(lambda x: x.get('end_date') if isinstance(x, dict) else None)
df['instant_date'] = df['context_details'].apply(lambda x: x.get('instant_date') if isinstance(x, dict) else None)
df.drop(columns=['context_details'], inplace=True)

df.drop_duplicates(inplace=True)


# def parse_date(date_string):
#     try:
#         date_obj = parser.parse(date_string)
#         return date_obj.strftime('%Y-%m-%d')
#     except ValueError:
#         return None


def extract_filed_as_of_date(html_path):
    directory = os.path.dirname(html_path)
    full_submission_path = os.path.join(directory, 'full-submission.txt')

    if not os.path.exists(full_submission_path):
        print(f"full-submission.txt not found in {directory}")
        return None

    with open(full_submission_path, 'r', encoding='utf-8') as f:
        text = f.read()

    match = re.search(r'FILED AS OF DATE:\s*(\d{8})', text)
    if match:
        filed_as_of_date = match.group(1)
        return f"{filed_as_of_date[:4]}-{filed_as_of_date[4:6]}-{filed_as_of_date[6:]}"

    return None


def extract_report_date(html_path):
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()

    soup = BeautifulSoup(html_content, 'html.parser')
    tags = soup.find_all(string=re.compile(r"(?i)For the (?:Quarterly|quarterly) period ended"))

    if tags:
        for tag in tags:
            next_siblings = tag.find_all_next(string=True, limit=5)
            combined_text = " ".join([tag] + [sibling.strip() for sibling in next_siblings])
            combined_text = combined_text.replace(u'\xa0', ' ').replace('&nbsp;', ' ')
            date_match = re.search(r"(\w+\s+\d{1,2},\s+\d{4})", combined_text)

            if date_match:
                date_string = date_match.group(0)
                try:
                    return date_parser.parse(date_string).strftime("%Y-%m-%d")
                except ValueError:
                    pass
    return None


# symbol = args.symbol
# report_date = extract_report_date(args.html_path)
# filing_date = extract_filed_as_of_date(args.html_path)
#
# print(symbol, report_date, filing_date)
#
# # Prepare values for database insertion
# symbol_id_query = f"(SELECT symbol_id FROM metadata.symbols WHERE symbol = '{symbol}')"
# report_date_id_query = f"(SELECT date_id FROM metadata.dates WHERE date = '{report_date}')" if report_date else "NULL"
# filing_date_id_query = f"(SELECT date_id FROM metadata.dates WHERE date = '{filing_date}')" if filing_date else "NULL"
#
# db_query = f"""
#     INSERT INTO financials.raw (symbol_id, report_date_id, filing_date_id, data)
#     VALUES (
#         {symbol_id_query},
#         {report_date_id_query},
#         {filing_date_id_query},
#         %s
#     )
#     ON CONFLICT (symbol_id, report_date_id, filing_date_id) DO NOTHING;
# """

# Execute the query with JSON data as a parameter
# db_connector.run_query(db_query, (df.to_json(orient='records'),), return_df=False)
