from db_credentials import DB_CREDENTIALS
import psycopg2

try:
    connection = psycopg2.connect(**DB_CREDENTIALS)
    print("Connection established")
except psycopg2.Error as e:
    print(e)
