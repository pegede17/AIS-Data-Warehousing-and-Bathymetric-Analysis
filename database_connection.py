from sshtunnel import SSHTunnelForwarder
import psycopg2
import configparser


def connect():

    config = configparser.ConfigParser()
    config.read('application.properties')

    connection = None

    tunnel = SSHTunnelForwarder(
        ('10.92.0.187', 22),
        ssh_username='ubuntu',
        ssh_private_key=config["Database"]["SSH_PATH"],
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


def connect_to_postgres_db():
    config = configparser.ConfigParser()
    config.read('application.properties')

    connection = None

    tunnel = SSHTunnelForwarder(
        ('10.92.0.187', 22),
        ssh_username='ubuntu',
        ssh_private_key=config["Database"]["SSH_PATH"],
        remote_bind_address=('localhost', 5432),
        local_bind_address=('localhost', 6543),  # could be any available port
    )

    # Start the tunnel
    tunnel.start()

    connection = psycopg2.connect(
        database="postgres",
        user=config["Database"]["dbuser"],
        password=config["Database"]["dbpass"],
        host=tunnel.local_bind_host,
        port=tunnel.local_bind_port,
    )
    return connection
