import psycopg2
import time
import yfinance

from db_credentials import DB_CREDENTIALS
from src.db_models.ticker import TickerActions


def get_stock_data_from_api(symbol: str):
    start_time = time.perf_counter()
    ticker = yfinance.Ticker(symbol)
    historical_data = ticker.history(period='5y')
    # Filter and reorder columns
    historical_data = historical_data[['Open', 'High', 'Low', 'Close', 'Volume']]
    historical_data.rename(columns={'Date': 'timestamp'}, inplace=True)
    end_time = time.perf_counter()
    print(f"Processed the api request in: {end_time - start_time}")
    return {'historical_data': historical_data, 'symbol': symbol}


def update_stocks_from_data_dict(dict):
    start_time = time.perf_counter()
    dataframe = dict['historical_data']
    data_to_insert = [(symbol, index) + tuple(row) for index, row in dataframe.iterrows()]
    with psycopg2.connect(**DB_CREDENTIALS) as connection:
        cursor = connection.cursor()
        # Insert data into the table
        insert_query = f'''
            INSERT INTO stocks (symbol, timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        '''
        cursor.executemany(insert_query, data_to_insert)
    end_time = time.perf_counter()
    print(f"Updated the database for {symbol} in: {end_time - start_time}")


def get_ticker_list() -> list[str]:
    ticker_actions = TickerActions(psycopg2.connect(**DB_CREDENTIALS))
    symbol_list = [ticker.stock_ticker for ticker in ticker_actions.get_all()]
    return symbol_list


if __name__ == '__main__':
    symbol_list = get_ticker_list()
    stocks_count = len(symbol_list)
    count = 0
    for symbol in symbol_list:
        try:
            stock_dict = get_stock_data_from_api(symbol)
            update_stocks_from_data_dict(stock_dict)
            count += 1
            print(f"Done with {symbol}. Updated {count} stocks")
            print(f"Stocks remaining: {stocks_count - count}")
        except:
            print(f"Could not fetch or insert data for {symbol}")
            continue
