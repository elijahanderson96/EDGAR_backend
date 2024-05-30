import pandas as pd
from bs4 import BeautifulSoup
import re
from dateutil import parser


def extract_filed_as_of_date(text):
    match = re.search(r'FILED AS OF DATE:\s*(\d{8})', text)
    if match:
        filed_as_of_date = match.group(1)
        formatted_date = f"{filed_as_of_date[:4]}-{filed_as_of_date[4:6]}-{filed_as_of_date[6:]}"
        return formatted_date
    return None


def get_conversion_factor(unit):
    if 'millions' in unit.lower():
        return 1000000
    elif 'thousands' in unit.lower():
        return 1000
    else:
        return 1


unit_pattern = re.compile(r'(\(.*?(?:millions|thousands).*?\))', re.IGNORECASE)

# Replace this with the actual file path from the directory structure
file_path = "sec-edgar-filings/GOOG/10-Q/0001652044-18-000035/primary-document.html"
# file_path = "sec-edgar-filings/PG/10-Q/0000080424-15-000098/full-submission.txt"

# Read the HTML file
with open(file_path, "r") as file:
    html_content = file.read()

# Create a BeautifulSoup object
soup = BeautifulSoup(html_content, "html.parser")
date = extract_filed_as_of_date(html_content)
unit_match = unit_pattern.search(html_content)

if unit_match:
    unit = unit_match.group(1)
    conversion_factor = get_conversion_factor(unit)
    print(f"Unit: {unit}")
    print(f"Conversion Factor: {conversion_factor}")
else:
    conversion_factor = 1
    print("Unit not found. Assuming no conversion needed.")

# Find the <font> tags containing the quarterly period text
font_tags = soup.find_all('font', string=re.compile(r"(?i)For the (?:Quarterly|quarterly) Period Ended"))

if font_tags:
    # Extract the text from the next <font> tag
    next_font_tag = font_tags[0].find_next('font')
    if next_font_tag:
        date_string = next_font_tag.get_text(strip=True)
        try:
            report_date = parser.parse(date_string).strftime("%Y-%m-%d")
            print("Quarterly Period Date:", report_date)
        except ValueError:
            print("Invalid date format.")
    else:
        print("Date not found in the next <font> tag.")
else:
    print("Quarterly period text not found in the HTML content.")

if not date:
    raise ValueError("Could not resolve filing date.")


def extract_tables(soup):
    tables = {}

    # List of possible texts for each financial statement
    financial_statements = [
        ("CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS", "CONSOLIDATED STATEMENTS OF OPERATIONS", "CONSOLIDATED STATEMENTS OF INCOME"),
        ("CONDENSED CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME", "CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME"),
        ("CONDENSED CONSOLIDATED BALANCE SHEETS", "CONSOLIDATED BALANCE SHEETS"),
        ("CONDENSED CONSOLIDATED STATEMENTS OF CASH FLOWS", "CONSOLIDATED STATEMENTS OF CASH FLOWS")
    ]

    for statement_variations in financial_statements:
        for statement_text in statement_variations:
            # Find the element containing the financial statement text
            element = soup.find(string=re.compile(statement_text, re.IGNORECASE))

            if element:
                # Find the parent element containing the financial statement text
                parent = element.find_parent()

                if parent:
                    # Check if there is an <a> tag with an href attribute within the parent element
                    a_tag = parent.find("a", href=True)
                    if a_tag:
                        # Navigate to the target element specified by the href
                        target_id = a_tag["href"].replace("#", "")
                        target_element = soup.find(id=target_id)
                        if target_element:
                            table = target_element.find_next("table")
                        else:
                            table = None
                    else:
                        # Find the next table after the parent element
                        table = parent.find_next("table")
                else:
                    table = None
            else:
                table = None

            if table:
                # Extract the table data
                table_data = []
                for row in table.find_all("tr"):
                    row_data = [cell.text.strip() for cell in row.find_all(["th", "td"])]
                    if any(row_data):
                        table_data.append(row_data)

                # Store the table data in the dictionary with the standardized key
                standard_key = statement_variations[0]
                tables[standard_key] = table_data
                break
            else:
                print(f"Table not found for the financial statement: {statement_text}")

    return tables

def clean_balance_sheet(table_data):
    # Remove empty strings from each row
    cleaned_data = [[cell for cell in row if cell != ''] for row in table_data]

    # Replace special characters or escape sequences
    cleaned_data = [[re.sub(r'\xa0', ' ', cell) for cell in row] for row in cleaned_data]

    # Extract timestamps
    timestamps = [re.sub(r'[^a-zA-Z0-9\s]', '', cell) for cell in cleaned_data[0] if cell != '']

    # Create column names for the DataFrame
    column_names = ['Metric'] + timestamps

    # Initialize an empty list to store the cleaned data
    cleaned_rows = []

    # Iterate over each row in the cleaned data (excluding the first row)
    for row in cleaned_data[1:]:
        metric = row[0]
        values = row[1:]

        # Remove any non-numeric characters (except for '-' and '.') from the values
        cleaned_values = [re.sub(r'[^0-9\-.]', '', cell) for cell in values]

        # Remove any nulls
        cleaned_values = [val for val in cleaned_values if val != '']

        # Pad the cleaned values with empty strings if necessary
        padded_values = cleaned_values + [''] * (len(column_names) - len(cleaned_values) - 1)

        # Append the metric and padded values to the cleaned rows
        cleaned_rows.append([metric] + padded_values)

    # Convert the cleaned rows to a pandas DataFrame
    df = pd.DataFrame(cleaned_rows, columns=column_names)

    # Set 'Metric' column as the index
    df.set_index('Metric', inplace=True)

    # Convert numeric values to float
    for col in df.columns:
        if col != 'Metric':
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def clean_income_statement(table_data):
    """Parse the first column (current) of the CONDENSED CONSOLIDATED STATEMENTS OF OPERATIONS within a quarterly
    report to a dataframe."""
    # Remove empty strings from each row
    cleaned_data = [[cell for cell in row if cell != ''] for row in table_data]

    # Replace special characters or escape sequences
    cleaned_data = [[re.sub(r'\xa0', ' ', cell) for cell in row] for row in cleaned_data]

    # Extract the current period and timestamp
    current_period = cleaned_data[0][0].strip()
    current_timestamp = re.sub(r'[^a-zA-Z0-9\s]', '', cleaned_data[1][0])

    # Create column names for the DataFrame
    column_names = ['Metric', f"{current_timestamp} ({current_period})"]

    # Initialize an empty list to store the cleaned data
    cleaned_rows = []

    # Iterate over each row in the cleaned data (excluding the first row)
    for row in cleaned_data[2:]:
        metric = row[0]
        values = row[1:]

        # Remove any non-numeric characters (except for '-' and '.') from the values
        cleaned_values = [re.sub(r'[^0-9\-.]', '', cell) for cell in values]

        # Remove any nulls
        cleaned_values = [val for val in cleaned_values if val != '']

        # Pad the cleaned values with empty strings if necessary
        padded_values = cleaned_values + [''] * (len(column_names) - len(cleaned_values) - 1)

        # Append the metric and padded values to the cleaned rows
        cleaned_rows.append([metric] + [padded_values[0]])

    # Convert the cleaned rows to a pandas DataFrame
    df = pd.DataFrame(cleaned_rows, columns=column_names)

    # Set 'Metric' column as the index
    df.set_index('Metric', inplace=True)

    # Convert numeric values to float
    for col in df.columns:
        if col != 'Metric':
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


tables = extract_tables(soup)
