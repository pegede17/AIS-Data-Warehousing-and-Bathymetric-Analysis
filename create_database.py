from database_connection import connect_to_postgres_db, connect
import configparser
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

config = configparser.ConfigParser()
config.read('application.properties')

connection = connect_to_postgres_db()

connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cur = connection.cursor()
cur.execute("CREATE DATABASE {};".format(
    config["Database"]["dbname"]
))
cur.close()
connection.close()

connection = connect()

connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = connection.cursor()
cur.execute("CREATE EXTENSION postgis;")
cur.close()
connection.close()
