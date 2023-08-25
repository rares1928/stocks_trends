import yfinance
import time

from src.load_db_from_api import get_stock_data_from_api


if __name__ == '__main__':
    start_time = time.perf_counter()
    ticker = yfinance.Ticker("IBM")
    historical_data = ticker.history(period='5d')

    print(get_stock_data_from_api('IBM'))
    # print(ticker.info)
    # print(historical_data)