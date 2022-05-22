from utils.create_database import create_database
import psycopg2
from utils.helper_functions import create_tables, get_fill_dim_cell_query
from utils.danish_waters import create_danish_waters_table
import pygrametl


def initialize_db(config):
    try:
        # Create Database
        print("Creating database")
        connection = create_database()

        # Create Tables
        print("Creating tables")
        commands = create_tables()
        danish_waters = create_danish_waters_table()

        cur = connection.cursor()
        for command in commands:
            cur.execute(command)
        for command in danish_waters:
            cur.execute(command)
            # close communication with the PostgreSQL database server

        print("Filling dim_cell")
        cur.execute(get_fill_dim_cell_query(config))
        cur.close()
        # commit the changes
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
