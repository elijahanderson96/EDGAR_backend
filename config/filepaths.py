import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

FILINGS_DIR = os.path.join(ROOT_DIR, 'sec-edgar-filings')

APP_LOGS = os.path.join(ROOT_DIR, 'logs')