import psycopg2
from sshtunnel import SSHTunnelForwarder
import configparser


def connect_to_db(config):
    if (config["Environment"]["connect_via_ssh"] == "True"):
        return connect_via_ssh()
    else:
        return connect_locally()


def connect_via_ssh():
    config = configparser.ConfigParser()
    config.read('../application.properties')

    print(config["Environment"]["SSH_PATH"])

    connection = None

    tunnel = SSHTunnelForwarder(
        ('130.225.39.233', 22),
        ssh_username='ubuntu',
        ssh_private_key=config["Environment"]["SSH_PATH"],
        remote_bind_address=('localhost', 5432),
        local_bind_address=('localhost', 6543),  # could be any available port
    )

    # Start the tunnel
    tunnel.start()

    connection = psycopg2.connect(
        database=config["Database"]["dbname"],
        user=config["Database"]["dbuser"],
        password=config["Database"]["dbpass"],
        host=tunnel.local_bind_host,
        port=tunnel.local_bind_port,
    )
    return connection


def connect_locally():
    config = configparser.ConfigParser()
    config.read('../application.properties')

    connection_string = "host='localhost' dbname='{}' user='{}' password='{}'".format(
        config["Database"]["dbname"],
        config["Database"]["dbuser"],
        config["Database"]["dbpass"]
    )

    connection = psycopg2.connect(connection_string)

    return connection
