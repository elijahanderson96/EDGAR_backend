from sec_edgar_downloader import Downloader
from edgar.symbols import symbols

symbols = symbols['symbol'].to_list()
# Download filings to the current working directory
dl = Downloader("Elijah", "elijahanderson96@gmail.com")

for symbol in symbols:
    print(f'Getting 10Q for {symbol}')
    dl.get("10-Q", symbol, download_details=True)

