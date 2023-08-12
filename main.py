from datetime import datetime
import psycopg2
import requests

from db_credentials import DB_CREDENTIALS
from src.db_models.stocks import StockData, StockActions


if __name__ == '__main__':
    print("hello")
    url = "https://alpha-vantage.p.rapidapi.com/query"
    querystring = {"symbol": "GOOGL", "function": "TIME_SERIES_INTRADAY", "interval": "60min", "outputsize": "full",
                   "month": "2020-07", "datatype": "json"}
    headers = {
        "X-RapidAPI-Key": "f12396277amsheb103e97f68e5f2p111cddjsn187c6ee9e05e",
        "X-RapidAPI-Host": "alpha-vantage.p.rapidapi.com"
    }
    try:
        api_response = requests.get(url, headers=headers, params=querystring)
    except ValueError as err:
        print(f"Could not get a response from api: {err}")
    print(api_response)
    print(api_response.headers)