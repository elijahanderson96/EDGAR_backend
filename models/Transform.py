# "Transform" the data. The transformation is spell checking, and then classifying (standardizing) the line items
# present within each of the statements.

from database.database import db_connector


datasets = {dataset: db_connector.run_query(f'SELECT data from financials.{dataset}')
            for dataset in ['balance_sheet', 'cash_flow', 'income']}

aggregated_data = []

for dataset, data in datasets.items():
    print(data)
