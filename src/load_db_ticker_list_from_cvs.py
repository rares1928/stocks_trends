import pandas as pd
import psycopg2
from datetime import datetime
from src.db_models.ticker import Ticker, TickerActions
from db_credentials import DB_CREDENTIALS


def ticker_list_from_path(path: str) -> list[Ticker]:
    # Read the CSV file into a DataFrame
    df = pd.read_csv(path)
    # Iterate over each row in the DataFrame and create Ticker instances
    ticker_instances = []
    for index, row in df.iterrows():
        stock_ticker = row['Symbol']
        market_cap = int(row['Market Cap'])
        last_modified = datetime.now()
        ticker = Ticker(stock_ticker, market_cap, last_modified)
        ticker_instances.append(ticker)
    return ticker_instances


if __name__ == '__main__':
    # print(ticker_list_from_path('~/Downloads/biggest-companies-stocks.csv'))
    ticker_actions = TickerActions(psycopg2.connect(**DB_CREDENTIALS))
    for path in [f"~/Downloads/biggest-companies-stocks({x}).csv" for x in range(1, 12)]:
        ticker_actions.create(ticker_list_from_path(path))
        print(f"Done with {path}")
