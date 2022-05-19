from utils.create_database import create_database
from utils.database_connection import connect_to_local, connect_via_ssh
import psycopg2
from utils.helper_functions import create_tables
from utils.dansk_farvand import create_dansk_farvand
import pygrametl


def initialize_db(config):
    try:
        if (config["Database"]["initialize"] == "True"):
            # Create Database
            print("Creating database")
            connection = create_database()

            # Create Tables
            print("Creating tables")
            commands = create_tables()
            dansk_farvand = create_dansk_farvand()

            cur = connection.cursor()
            for command in commands:
                cur.execute(command)
            for command in dansk_farvand:
                cur.execute(command)
                # close communication with the PostgreSQL database server
            cur.close()
            # commit the changes
            connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
