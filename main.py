import psycopg2
import yfinance
import time
import pandas as pd

from db_credentials import DB_CREDENTIALS
from src.db_models.ticker import TickerActions
from src.load_db_from_api import get_stock_data_from_api


if __name__ == '__main__':
    # start_time = time.perf_counter()
    # ticker = yfinance.Ticker("IBM")
    # historical_data = ticker.history(period='5d')
    #
    # print(get_stock_data_from_api('IBM'))
    # # print(ticker.info)
    # # print(historical_data)

    ticker_actions = TickerActions(psycopg2.connect(**DB_CREDENTIALS))
    print(ticker_actions.get([1]))
