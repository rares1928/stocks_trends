from datetime import datetime
import psycopg2
import requests
import time

from db_credentials import DB_CREDENTIALS
from src.db_models.stocks import StockData, StockActions


def get_json_from_api(month: str, symbol: str) -> dict:
    start_time = time.perf_counter()
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&month={month}&outputsize=full&interval=60min&apikey=ZFFDFON6BSFJQ11P"
    try:
        api_response = requests.get(url, headers=headers, params=querystring)
    except ValueError as err:
        print(f"Could not get a response from api: {err}")
    try:
        api_response.json()['Meta Data']['2. Symbol']
    except KeyError as err:
        print(f"Could not find ['Meta Data']['2. Symbol'] for {symbol}:{month}, the response is: {api_response}.\n"
              f"The headers are: {api_response.headers}\n"
              f"Retrying...")
        time.sleep(60)
        return get_json_from_api(month=month, symbol=symbol)
    end_time = time.perf_counter()
    print(f"Processed the api request in: {end_time-start_time}")
    return api_response.json()


def update_stocks_from_json(json):
    start_time = time.perf_counter()
    stocks_list = []
    symbol = json['Meta Data']['2. Symbol']
    timestamps = [timestamp for timestamp in json['Time Series (60min)'].keys()]
    for timestamp in timestamps:
        new_stock = StockData(
            symbol=symbol,
            timestamp=timestamp,
            open=json['Time Series (60min)'][timestamp]['1. open'],
            high=json['Time Series (60min)'][timestamp]['2. high'],
            low=json['Time Series (60min)'][timestamp]['3. low'],
            close=json['Time Series (60min)'][timestamp]['4. close'],
            volume=json['Time Series (60min)'][timestamp]['5. volume'],
        )
        stocks_list.append(new_stock)
    with psycopg2.connect(**DB_CREDENTIALS) as connection:
        StockActions(connection=connection).create(stocks_list)
    end_time = time.perf_counter()
    print(f"Updated the database for {symbol} in: {end_time-start_time}")


if __name__ == '__main__':
    months_list = [f"{year:04d}-{month:02d}" for year in range(2020, 2024) for month in range(1, 13)]
    symbols_list = ["MSFT","AAPL", "AMZN","GOOGL", "META", "TSLA", "BRK-B", "JPM", "V", "JNJ"]
    symbol_count = 0
    for symbol in symbols_list:
        symbol_count += 1
        count = 0
        for month in months_list:
            json = get_json_from_api(month=month, symbol=symbol)
            update_stocks_from_json(json)
            count += 1
            print(f"Done with {symbol}:{month}")
            print(f"Months remaining: {len(months_list) - count}, stocks remaining: {len(symbols_list) - symbol_count}")

# symbols done: [MSFT,"AAPL", "AMZN","GOOGL", "META", "TSLA", "BRK-B", "JPM",]
# symbols with problems: []
