import psycopg2
from psycopg2 import sql
from datetime import datetime



class Ticker:
    id: int
    stock_ticker: str
    market_cap: int
    last_modified: datetime

    def __init__(self, stock_ticker: str, market_cap: int, last_modified: datetime, id=None):
        self.id = id
        self.stock_ticker = stock_ticker
        self.market_cap = market_cap
        self.last_modified = last_modified

    def __repr__(self):
        return (
            f"Ticker(id={self.id}, stock_ticker='{self.stock_ticker}', "
            f"market_cap={self.market_cap}, last_modified='{self.last_modified}')"
        )


class TickerActions:
    def __init__(self, connection):
        self.connection = connection

    def get_all(self):
        try:
            cursor = self.connection.cursor()
            # Execute a SELECT query to retrieve all tickers from the 'tickers' table
            cursor.execute("SELECT * FROM tickers")
            # Fetch all the rows and create Ticker objects
            tickers = []
            for row in cursor.fetchall():
                id, stock_ticker, market_cap, last_modified = row
                tickers.append(Ticker(id=id, stock_ticker=stock_ticker, market_cap=market_cap, last_modified=last_modified))
            return tickers
        except Exception as e:
            # Handle exceptions appropriately (e.g., logging, error handling)
            print(f"Error: {e}")
            return []

    def create(self, tickers):
        try:
            cursor = self.connection.cursor()
            insert_query = sql.SQL("""
                INSERT INTO tickers (stock_ticker, market_cap, last_modified)
                VALUES (%s, %s, %s)
            """)
            for ticker in tickers:
                cursor.execute(insert_query, (ticker.stock_ticker, ticker.market_cap, ticker.last_modified))
            self.connection.commit()
            print("Ticker records created successfully.")
        except (Exception, psycopg2.Error) as error:
            print("Error creating Ticker records:", error)

    def get(self, stock_ticker_ids: list[int]) -> list[Ticker]:
        print(stock_ticker_ids)
        placeholders = ', '.join(['%s'] * len(stock_ticker_ids))
        query = f'SELECT * FROM tickers WHERE id IN ({placeholders})'
        print(query)
        with self.connection.cursor() as cursor:
            cursor.execute(query, stock_ticker_ids)
            rows = cursor.fetchall()
            ticker_data_list = [
                Ticker(
                    id=row[0],
                    stock_ticker=row[1],
                    market_cap=row[2],
                    last_modified=row[3],
                ) for row in rows
            ]
            return ticker_data_list

    def update(self, tickers):
        try:
            cursor = self.connection.cursor()
            for ticker in tickers:
                update_query = sql.SQL("""
                    UPDATE tickers
                    SET market_cap = %s, last_modified = %s
                    WHERE stock_ticker = %s
                """)
                cursor.execute(update_query, (ticker.market_cap, ticker.last_modified, ticker.stock_ticker))
            self.connection.commit()
            print("Ticker records updated successfully.")
        except (Exception, psycopg2.Error) as error:
            print("Error updating Ticker records:", error)



# # How to use the above functions:
# ticker_actions = TickerActions(psycopg2.connect(**DB_CREDENTIALS))
#
# # # Create a list of Ticker instances and insert them into the database
# # new_tickers = [
# #     Ticker("AAPL", 2200000000000, datetime(2023, 1, 3)),
# #     Ticker("GOOGL", 1800000000000, datetime(2023, 1, 4)),
# # ]
# #
# # ticker_actions.create(new_tickers)
#
# # Get a list of Ticker records from the database
# retrieved_tickers = ticker_actions.get([1,2])
# for retrieved_ticker in retrieved_tickers:
#     print(retrieved_ticker)
#
# # Update a list of Ticker records in the database
# updated_tickers = [
#     Ticker("AAPL", 2300000000000, datetime(2023, 1, 5)),
#     Ticker("GOOGL", 1900000000000, datetime(2023, 1, 6)),
# ]
#
# ticker_actions.update(updated_tickers)
