import psycopg2
import pygrametl

dw_string = "host='localhost' dbname='postgres' user='postgres' password='admin'"
connection = psycopg2.connect(dw_string)
dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

cur = connection.cursor()
cur.execute('CREATE DATABASE p9-test;')
cur.execute("CREATE EXTENSION postgis;")
cur.close()
connection.close()