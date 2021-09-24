from create_database import create_database
from database_connection import connect_to_local, connect_via_ssh
import psycopg2
import pygrametl
from pygrametl.datasources import SQLSource, CSVSource
from pygrametl.tables import BulkFactTable, Dimension, CachedDimension, FactTable, SlowlyChangingDimension
from sshtunnel import SSHTunnelForwarder
from helper_functions import create_tables
from pygrametl.datasources import SQLSource, CSVSource, ProcessSource, TransformingSource
from pygrametl.tables import BulkFactTable, DecoupledFactTable, DimensionPartitioner, DecoupledDimension, Dimension, CachedDimension, FactTable, SlowlyChangingDimension
from datetime import datetime
import configparser
from pygrametl.parallel import shareconnectionwrapper, getsharedsequencefactory
from pygrametl import ConnectionWrapper
from datetime import datetime
import configparser
# from helper_functions import create_tables


def load_data_into_db():

    config = configparser.ConfigParser()
    config.read('application.properties')

    # Initialize variables
    connection = None
    dw_conn_wrapper = None

    # Initialize database
    try:
        if (config["Database"]["initialize"] == "True"):
            # Create Database
            connection = create_database()

            # Create Tables
            commands = create_tables()

            cur = connection.cursor()
            for command in commands:
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
        global connection
        cursor = connection.cursor()
        cursor.copy_from(file=filehandle, table=name, sep=fieldsep, null=str(nullval),
                         columns=attributes)

    # Creation of dimension and fact table abstractions for use in the ETL flow
    date_dimension = CachedDimension(
        name='date',
        key='date_id',  # Lav den til en smartkey.
        attributes=['millennium', 'century', 'decade', 'iso_year', 'year', 'month', 'day',
                    'day_of_week', 'iso_day_of_week', 'day_of_year', 'quarter', 'epoch', 'week'],
        prefill=True,
        cacheoninsert=True,
        size=0
    )

    time_dimension = CachedDimension(
        name='time',
        key='time_id',  # Lav den til en smartkey.
        attributes=['hour', 'minute', 'second'],
            prefill=True,
        cacheoninsert=True,
        size=0
    )

    ship_dimension = CachedDimension(
        name='ship',
        key='ship_id',
        attributes=['MMSI', 'IMO', 'Name', 'Width', 'Length', 'Callsign',
                    'Draught', 'size_a', 'size_b', 'size_c', 'size_d'],
        cachefullrows=True,
        prefill=True,
        cacheoninsert=True
    )

    ship_type_dimension = CachedDimension(
        name='ship_type',
        key="ship_type_id",
        attributes=['ship_type'],
        prefill=True,
        cacheoninsert=True,

    )

    type_of_position_fixing_device_dimension = CachedDimension(
        name='type_of_position_fixing_device',
        key='type_of_position_fixing_device_id',
        attributes=['device_type'],
        prefill=True,
        cacheoninsert=True,

    )

    cargo_type_dimension = CachedDimension(
        name='cargo_type',
        key='cargo_type_id',
        attributes=['cargo_type'],
        prefill=True,
        cacheoninsert=True,

    )

    navigational_status_dimension = CachedDimension(
        name='navigational_status',
        key='navigational_status_id',
        attributes=['navigational_status'],
        prefill=True,
        cacheoninsert=True,

    )

    type_of_mobile_dimension = CachedDimension(
        name='type_of_mobile',
        key='type_of_mobile_id',
        attributes=['mobile_type'],
        prefill=True,
        cacheoninsert=True,

    )

    destination_dimension = CachedDimension(
        name='destination',
        key='destination_id',
        attributes=['user_defined_destination', 'mapped_destination'],
        prefill=True,
        cacheoninsert=True,

    )

    data_source_type_dimension = CachedDimension(
        name='data_source_type',
        key='data_source_type_id',
        attributes=['data_source_type'],
        prefill=True,
        cacheoninsert=True,

    )

    audit_dimension = Dimension(
        name='audit',
        key='audit_id',
        attributes=['timestamp', 'processed_records', 'source_system',
                    'etl_version', 'table_name', 'comment', ]
    )

    fact_table = BulkFactTable(
        name='fact_table',
        keyrefs=['eta_date_id', 'eta_time_id', 'ship_id', 'ts_date_id', 'ts_time_id', 'data_source_type_id', 'destination_id',
                 'type_of_mobile_id', 'navigational_status_id', 'cargo_type_id', 'type_of_position_fixing_device_id', 'ship_type_id', 'audit_id'],
        measures=['coordinate', 'rot', 'sog', 'cog', 'heading'],
        bulkloader=pgbulkloader,
        fieldsep=',',
        rowsep='\\r\n',
        nullsubst=str(None),
        bulksize=500000,
        usefilename=False,
    )

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
        config["Environment"]["FILE_PATH"], 'r')
    ais_source = CSVSource(f=ais_file_handle, delimiter=',')

    transformeddata = TransformingSource(ais_source, transformNulls)

    # inputdata = ProcessSource(transformeddata, batchsize=500, queuesize=10)

    i = 0
    for row in transformeddata:
        i = i + 1
        if (i % 10000 == 0):
            print(str(datetime.now()) + " Reached milestone: " + str(i))

        fact = {}
        fact["audit_id"] = audit_id
        fact["ship_id"] = ship_dimension.ensure(row, {
            'size_a': 'A',
            'size_b': 'B',
            'size_c': 'C',
            'size_d': 'D',
        })

        fact["ship_type_id"] = ship_type_dimension.ensure(row, {
            'ship_type': 'Ship type'
        })

        fact["type_of_position_fixing_device_id"] = type_of_position_fixing_device_dimension.ensure(row, {
            'device_type': 'Type of position fixing device'
        })

        fact["cargo_type_id"] = cargo_type_dimension.ensure(row, {
            'cargo_type': 'Cargo type'
        })

        fact["navigational_status_id"] = navigational_status_dimension.ensure(row, {
            'navigational_status': 'Navigational status'
        })

        fact["type_of_mobile_id"] = type_of_mobile_dimension.ensure(row, {
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
            'ts_time_id': convertTimestampToTimeId(row["# Timestamp"]),
            'coordinate': ("POINT(" + row["Longitude"] + " " + row["Latitude"] + ")"),
            'rot': row["ROT"],
            'sog': row["SOG"],
            'cog': row["COG"],
            'heading': validateToZero(row["Heading"])
        }

        fact.update(fact_extra)  # Adds the extra attributes to the fact object

        fact_table.insert(fact)

    audit_obj['processed_records'] = i
    audit_obj['audit_id'] = audit_id
    audit_dimension.update(audit_obj)

    dw_conn_wrapper.commit()
    dw_conn_wrapper.close()

    print(datetime.now())
