import psycopg2
import configparser
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sshtunnel import SSHTunnelForwarder


config = configparser.ConfigParser()
config.read('application.properties')

tunnel = SSHTunnelForwarder(
    ('10.92.0.187', 22),
    ssh_username='ubuntu',
    ssh_private_key='C:\\Users\\Peter\\Desktop\\secret.pem',
    remote_bind_address=('localhost', 5432),
    local_bind_address=('localhost', 6543),  # could be any available port
)
# Start the tunnel
tunnel.start()

dw_string = "host=tunnel.local_bind_host port=tunnel.local_bind_port dbname='postgres' user='postgres' password='admin'"
connection = psycopg2.connect(
    database='postgres',
    user='postgres',
    password='admin',
    host=tunnel.local_bind_host,
    port=tunnel.local_bind_port,
)

connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

cur = connection.cursor()
cur.execute("CREATE DATABASE {};".format(
    config["Database"]["dbname"]
))
cur.close()
connection.close()

connection = psycopg2.connect(
    database=config["Database"]["dbname"],
    user='postgres',
    password='admin',
    host=tunnel.local_bind_host,
    port=tunnel.local_bind_port,)

connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = connection.cursor()
cur.execute("CREATE EXTENSION postgis;")
cur.close()
connection.close()
tunnel.stop()
