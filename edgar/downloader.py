from sec_edgar_downloader import Downloader

# Download filings to the current working directory
dl = Downloader("Elijah", "elijahanderson96@gmail.com")

# Get all 10-K filings for Microsoft without the filing details
dl.get("10-Q", "MSFT", download_details=True)

# Get the latest supported filings, if available, for Apple
# for filing_type in dl.supported_filings:
#     dl.get(filing_type, "AAPL", limit=1)
