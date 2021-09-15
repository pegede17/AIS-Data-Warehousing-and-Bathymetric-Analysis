import psycopg2
import configparser
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

config = configparser.ConfigParser()
config.read('application.properties')

dw_string = "host='{}' dbname='postgres' user='{}' password='{}'".format(
    config["Database"]["hostname"],
    config["Database"]["dbuser"],
    config["Database"]["dbpass"],
)
connection = psycopg2.connect(dw_string)

connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cur = connection.cursor()
cur.execute('CREATE DATABASE "{}";'.format(config["Database"]["dbname"]))
cur.close()
connection.close()

dw_string = "host='{}' dbname='{}' user='{}' password='{}'".format(
    config["Database"]["hostname"],
    config["Database"]["dbname"],
    config["Database"]["dbuser"],
    config["Database"]["dbpass"],
)
connection = psycopg2.connect(dw_string)
connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = connection.cursor()
cur.execute("CREATE EXTENSION postgis;")
cur.close()
connection.close()