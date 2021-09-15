import psycopg2
import pygrametl
import configparser

config = configparser.ConfigParser()
config.read('application.properties')

dw_string = "host='{}' dbname='postgres' user='{}' password='{}'".format(
    config["Database"]["hostname"],
    config["Database"]["dbuser"],
    config["Database"]["dbpass"],
)
connection = psycopg2.connect(dw_string)
dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

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
cur = connection.cursor()
cur.execute("CREATE EXTENSION postgis;")
cur.close()
connection.close()