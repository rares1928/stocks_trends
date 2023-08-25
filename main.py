from datetime import datetime
import psycopg2
import requests

from src.db_models.db_credentials import DB_CREDENTIALS
from src.db_models.stocks import StockData, StockActions


if __name__ == '__main__':
    # replace the "demo" apikey below with your own key from https://www.alphavantage.co/support/#api-key
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=IBM&outputsize=full&interval=60min&apikey=ZFFDFON6BSFJQ11P"
    api_response = requests.get(url)

    print(api_response)
    print(api_response.headers)
    print(api_response.json())