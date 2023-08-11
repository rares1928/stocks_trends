from datetime import datetime
import psycopg2
import requests

from db_credentials import DB_CREDENTIALS
from src.db_models.stocks import StockData, StockActions


if __name__ == '__main__':
    print("hello")
