from create_database import create_database
from dansk_farvand import create_dansk_farvand
from database_connection import connect_to_local, connect_via_ssh
import psycopg2
import pygrametl
from pygrametl.datasources import SQLSource, CSVSource
from pygrametl.tables import BulkFactTable, Dimension, CachedDimension, FactTable, SlowlyChangingDimension
from sshtunnel import SSHTunnelForwarder
from helper_functions import create_audit_dimension, create_cargo_type_dimension, create_data_source_type_dimension, create_date_dimension, create_destination_dimension, create_fact_table, create_navigational_status_dimension, create_ship_dimension, create_ship_type_dimension, create_tables, create_time_dimension, create_type_of_mobile_dimension, create_type_of_position_fixing_device_dimension
from pygrametl.datasources import SQLSource, CSVSource, ProcessSource, TransformingSource
from pygrametl.tables import BulkFactTable, DecoupledFactTable, DimensionPartitioner, DecoupledDimension, Dimension, CachedDimension, FactTable, SlowlyChangingDimension
from datetime import datetime
import configparser
from pygrametl.parallel import shareconnectionwrapper, getsharedsequencefactory
from pygrametl import ConnectionWrapper
import configparser
# from helper_functions import create_tables


def load_data_into_db(config):

    # Initialize variables
    connection = None
    dw_conn_wrapper = None

    # Initialize database
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
    finally:
        if connection is not None:
            dw_conn_wrapper = pygrametl.ConnectionWrapper(
                connection=connection)
        else:
            if(config["Environment"]["development"] == "True"):
                connection = connect_via_ssh()
            else:
                connection = connect_to_local()
            dw_conn_wrapper = pygrametl.ConnectionWrapper(
                connection=connection)

    def convertTimestampToTimeId(timestamp):
        if(timestamp):
            date = datetime.strptime(timestamp, "%d/%m/%Y %H:%M:%S")
            formatted = int(date.strftime("%H%M%S"))
            return formatted
        else:
            return 0

    def convertTimestampToDateId(timestamp):
        if(timestamp):
            date = datetime.strptime(timestamp, "%d/%m/%Y %H:%M:%S")
            return int(date.strftime("%Y%m%d"))
        else:
            return 0

    def pgbulkloader(name, attributes, fieldsep, rowsep, nullval, filehandle):
        cursor = connection.cursor()
        cursor.copy_from(file=filehandle, table=name, sep=fieldsep, null=str(nullval),
                         columns=attributes)

    # Creation of dimension and fact table abstractions for use in the ETL flow

    ship_type_dimension = create_ship_type_dimension()

    type_of_position_fixing_device_dimension = create_type_of_position_fixing_device_dimension()

    cargo_type_dimension = create_cargo_type_dimension()

    type_of_mobile_dimension = create_type_of_mobile_dimension()

    ship_dimension = create_ship_dimension(type_of_position_fixing_device_dimension, ship_type_dimension, type_of_mobile_dimension)

    navigational_status_dimension = create_navigational_status_dimension()

    destination_dimension = create_destination_dimension()

    data_source_type_dimension = create_data_source_type_dimension()

    audit_dimension = create_audit_dimension()

    fact_table = create_fact_table(
        pgbulkloader=pgbulkloader, tb_name="fact_ais")

    audit_obj = {'timestamp': datetime.now(),
                 'processed_records': 0,
                 'source_system': config["Audit"]["source_system"],
                 'etl_version': config["Audit"]["elt_version"],
                 'table_name': fact_table.name,
                 'comment': config["Audit"]["comment"]}

    audit_id = audit_dimension.insert(audit_obj)

    print(datetime.now())

    def validateToNull(val): return None if (
        val == "" or val == "Unknown") else val

    def validateToZero(val): return 0 if (
        val is None or val == "" or val == "Unknown") else val

    def transformNulls(row):
        for value in row:
            val = row[value]
            row[value] = validateToNull(val)

    ais_file_handle = open(
        config["Environment"]["FILE_PATH"] + config["Environment"]["FILE_NAME"], 'r')
    ais_source = CSVSource(f=ais_file_handle, delimiter=',')

    transformeddata = TransformingSource(ais_source, transformNulls)

    i = 0
    
    print("Starting loading " + str(datetime.now()))
    for row in transformeddata:
        timestamp = convertTimestampToTimeId(row["# Timestamp"])
        if (timestamp < 10000): # Start loading after 10 minutes to bypass null values at midnight
            continue
        
        i = i + 1

        fact = {}
        fact["audit_id"] = audit_id

        row['MMSI'] = int(row['MMSI'])
        fact['cell_id'] = 1

        fact["ship_id"] = ship_dimension.ensure(row, {
            'size_a': 'A',
            'size_b': 'B',
            'size_c': 'C',
            'size_d': 'D',
            'mmsi': 'MMSI',
            'ship_type': 'Ship type',
            'device_type': 'Type of position fixing device',
            'mobile_type': 'Type of mobile'
        })

        fact["ship_type_id"] = ship_type_dimension.lookup(row, {
            'ship_type': 'Ship type'
        })

        fact["type_of_position_fixing_device_id"] = type_of_position_fixing_device_dimension.lookup(row, {
            'device_type': 'Type of position fixing device'
        })

        fact["cargo_type_id"] = cargo_type_dimension.ensure(row, {
            'cargo_type': 'Cargo type'
        })

        fact["navigational_status_id"] = navigational_status_dimension.ensure(row, {
            'navigational_status': 'Navigational status'
        })

        fact["type_of_mobile_id"] = type_of_mobile_dimension.lookup(row, {
            'mobile_type': 'Type of mobile'
        })

        fact["destination_id"] = destination_dimension.ensure(row, {
            'user_defined_destination': 'Destination',
            'mapped_destination': 'Destination'
        })

        fact["data_source_type_id"] = data_source_type_dimension.ensure(row, {
            'data_source_type': 'Data source type'
        })

        # Retrieve attributes that are obtained either by formula or as a measure from dataset

        fact_extra = {
            'eta_date_id': convertTimestampToDateId(row["ETA"]),
            'eta_time_id': convertTimestampToTimeId(row["ETA"]),
            'ts_date_id': convertTimestampToDateId(row["# Timestamp"]),
            'ts_time_id': timestamp,
            'coordinate': ("POINT(" + row["Longitude"] + " " + row["Latitude"] + ")"),
            'draught': row["Draught"],
            'rot': row["ROT"],
            'sog': row["SOG"],
            'cog': row["COG"],
            'heading': validateToZero(row["Heading"]),
            'longitude': float(row['Longitude']),
            'latitude': float(row['Latitude'])
        }

        fact.update(fact_extra)  # Adds the extra attributes to the fact object

        fact_table.insert(fact)

    audit_obj['processed_records'] = i
    audit_obj['audit_id'] = audit_id
    audit_dimension.update(audit_obj)

    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()

    print(datetime.now())
