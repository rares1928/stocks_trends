import psycopg2
import time
import yfinance

from src.db_models.db_credentials import DB_CREDENTIALS


def get_stock_data_from_api(symbol: str):
    start_time = time.perf_counter()
    ticker = yfinance.Ticker(symbol)
    historical_data = ticker.history(period='5y')
    # Filter and reorder columns
    historical_data = historical_data[['Open', 'High', 'Low', 'Close', 'Volume']]
    historical_data.rename(columns={'Date': 'timestamp'}, inplace=True)
    end_time = time.perf_counter()
    print(f"Processed the api request in: {end_time-start_time}")
    return {'historical_data': historical_data, 'market_cap': ticker.info['marketCap'] if ticker.info['marketCap'] else 0,  'symbol': symbol}


def update_stocks_from_data_dict(dict):
    start_time = time.perf_counter()
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
    print(f"Updated the database for {symbol} in: {end_time-start_time}")


if __name__ == '__main__':
    symbols_list = ["MSFT","AAPL", "AMZN","GOOGL", "META", "TSLA", "BRK-B", "JPM", "V", "JNJ"]
    symbol_count = 0
    for symbol in symbols_list:
        symbol_count += 1
        count = 0
        dict = get_stock_data_from_api(symbol)
        # update_stocks_from_data_dict(dict)
        count += 1
        print(f"Done with {symbol}")
        print(f"Stocks remaining: {len(symbols_list) - symbol_count}")
