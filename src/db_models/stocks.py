from datetime import datetime


class StockData:
    id: int
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    last_modified: datetime
    index: str

    def __repr__(self):
        return (
            f"StockData(id={self.id}, symbol='{self.symbol}', timestamp={self.timestamp}, "
            f"open_price={self.open}, high={self.high}, low={self.low}, "
            f"close={self.close}, volume={self.volume}, last_modified={self.last_modified}, "
            f"index='{self.index}')"
        )

    def __init__(self, symbol, timestamp, open, high, low, close, volume, index=None, last_modified=datetime.now(), id=None):
        self.id = id
        self.symbol = symbol
        self.timestamp = timestamp
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.last_modified = last_modified
        self.index = index


class StockActions:
    def __init__(self, connection):
        self.connection = connection

    def create(self, stock_data_list: list[StockData]):
        query = '''
        INSERT INTO stocks (symbol, timestamp, open, high, low, close, volume, index)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id
        '''
        values_list = [
            (
                stock_data.symbol, stock_data.timestamp, stock_data.open, stock_data.high,
                stock_data.low, stock_data.close, stock_data.volume, stock_data.index
            )
            for stock_data in stock_data_list
        ]
        with self.connection.cursor() as cursor:
            cursor.executemany(query, values_list)
        self.connection.commit()

    def get(self, stock_ids: list[int]) -> list[StockData]:
        placeholders = ', '.join(['%s'] * len(stock_ids))
        query = f'SELECT * FROM stocks WHERE id IN ({placeholders})'
        with self.connection.cursor() as cursor:
            cursor.execute(query, stock_ids)
            rows = cursor.fetchall()
            stock_data_list = [
                StockData(
                    id=row[0],
                    symbol=row[1],
                    timestamp=row[2],
                    open=row[3],
                    high=row[4],
                    low=row[5],
                    close=row[6],
                    volume=row[7],
                    last_modified=row[8],
                    index=row[9],
                ) for row in rows
            ]
            return stock_data_list

    def update(self, stock_data_list: list[StockData]) -> list[StockData]:
        query = '''
        UPDATE stocks
        SET symbol = %s, timestamp = %s, open = %s, high = %s, low = %s,
            close = %s, volume = %s, index = %s
        WHERE id = %s
        '''
        values_list = [
            (
                stock_data.symbol, stock_data.timestamp, stock_data.open, stock_data.high,
                stock_data.low, stock_data.close, stock_data.volume,
                stock_data.index, stock_data.id
            )
            for stock_data in stock_data_list
        ]
        with self.connection.cursor() as cursor:
            cursor.executemany(query, values_list)
        self.connection.commit()
        return self.get([stock.id for stock in stock_data_list])

    def delete(self, stock_ids: list[int]):
        placeholders = ', '.join(['%s'] * len(stock_ids))
        query = f'DELETE FROM stocks WHERE id IN ({placeholders})'
        with self.connection.cursor() as cursor:
            cursor.execute(query, stock_ids)
        self.connection.commit()
