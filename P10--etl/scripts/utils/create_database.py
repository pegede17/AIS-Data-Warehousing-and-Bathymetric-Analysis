from utils.database_connection import connect_to_local, connect_via_ssh, connect_to_postgres_db_local, connect_to_postgres_db_via_ssh
from utils.helper_functions import get_histogram_functions_query, get_fill_dim_cell_query
import configparser
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_database():
    config = configparser.ConfigParser()
    config.read('../application.properties')

    if(config["Environment"]["development"] == "True"):
        connection = connect_to_postgres_db_via_ssh()
    else:
        connection = connect_to_postgres_db_local()

    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cur = connection.cursor()
    cur.execute("CREATE DATABASE {};".format(
        config["Database"]["dbname"]
    ))
    cur.close()
    connection.close()

    if(config["Environment"]["development"] == "True"):
        connection = connect_via_ssh()
    else:
        connection = connect_to_local()

    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = connection.cursor()
    cur.execute("CREATE EXTENSION postgis;")
    cur.execute("CREATE EXTENSION postgis_raster;")
    cur.execute(get_histogram_functions_query())
    print("Filling dim_cell")
    cur.execute(get_fill_dim_cell_query())
    cur.close()

    return connection
