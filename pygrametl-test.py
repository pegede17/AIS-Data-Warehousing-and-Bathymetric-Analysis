import psycopg2
import pygrametl
from pygrametl.datasources import SQLSource, CSVSource
from pygrametl.tables import BulkFactTable, Dimension, CachedDimension, FactTable, SlowlyChangingDimension
from helper_functions import create_tables
from datetime import datetime
import configparser
import time

# Parse configuration file
config = configparser.ConfigParser()
config.read('application.properties')

connection = None

dw_string = "host='{}' dbname='{}' user='{}' password='{}'".format(
    config["Database"]["hostname"],
    config["Database"]["dbname"],
    config["Database"]["dbuser"],
    config["Database"]["dbpass"],
)
connection = psycopg2.connect(dw_string)
dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)
ais_file_handle = open('aisdk-2021-07/aisdk-2021-07-31.csv', 'r')
ais_source = CSVSource(f=ais_file_handle, delimiter=',')

# psycopg initialization

commands = create_tables()

try:
    cur = connection.cursor()
    for command in commands:
        cur.execute(command)
        # close communication with the PostgreSQL database server
    # cur.close()
        # commit the changes
    connection.commit()
except (Exception, psycopg2.DatabaseError) as error:
        print(error)
# finally:
#     if connection is not None:
#         connection.close()

# Function to extract proper id from a timestamp
def convertTimestampToTimeId(timestamp):
    if(timestamp != ""):
        date = datetime.strptime(timestamp, "%d/%m/%Y %H:%M:%S")
        return int(date.strftime("%H%M%S"))
    else:
        return 0

def convertTimestampToDateId(timestamp):
    if(timestamp != ""):
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
    key='date_id', #Lav den til en smartkey.
    attributes=['millennium', 'century', 'decade', 'iso_year', 'year', 'month', 'day', 'day_of_week', 'iso_day_of_week', 'day_of_year', 'quarter', 'epoch', 'week'],
    prefill=True,
    cacheoninsert=True,
    )

time_dimension = CachedDimension(
    name='time',
    key='time_id', #Lav den til en smartkey.
    attributes=['hour', 'minute', 'second'],
        prefill=True,
    cacheoninsert=True,
)

ship_dimension = CachedDimension(
    name='ship',
    key='ship_id',
    attributes=['mmsi', 'imo', 'name', 'width', 'length', 'callsign', 'draught', 'size_a', 'size_b', 'size_c', 'size_d'],
        prefill=True,
    cacheoninsert=True,
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

fact_table = BulkFactTable(
    name='fact_table',
    keyrefs=['eta_date_id', 'eta_time_id', 'ship_id', 'ts_date_id', 'ts_time_id','data_source_type_id', 'destination_id', 'type_of_mobile_id', 'navigational_status_id', 'cargo_type_id', 'type_of_position_fixing_device_id', 'ship_type_id'],
    measures=['coordinate', 'rot', 'sog', 'cog', 'heading', ],
    bulkloader=pgbulkloader,
    fieldsep=',', 
    rowsep='\\r\n', 
    nullsubst=str(None), 
    tempdest=None, 
    bulksize=500000, 
    usefilename=False, 
    strconverter=pygrametl.getdbfriendlystr, 
    encoding=None, 
    dependson=()
)

print(datetime.now())

i = 0
for row in ais_source:
    i = i + 1
    if (i % 100000 == 0):
        print("Reached milestone: " + str(i)) 
        print(datetime.now())

    t0 = time.time()
    
    imo = row["IMO"] if row["IMO"] != "Unknown" else None
    name = row["Name"] if row["Name"] != "" else None
    width = row["Width"] if row["Width"] != "" else None
    length = row["Length"] if row["Length"] != "" else None
    callsign =  row["Callsign"] if row["Callsign"] != "" else None
    draught = row["Draught"] if row["Draught"] != "" else None

    size_a = row["A"] if row["A"] != "" else None
    size_b = row["B"] if row["B"] != "" else None
    size_c = row["C"] if row["C"] != "" else None
    size_d = row["D"] if row["D"] != "" else None
    ship_dimension_id = ship_dimension.ensure(        
        {
            'mmsi': row["MMSI"],
            'imo':  imo,
            'name': name,
            'width': width,
            'length': length,
            'callsign': callsign,
            'draught': draught,
            'size_a': size_a,
            'size_b': size_b,
            'size_c': size_c,
            'size_d': size_d
        }
    )
    t1 = time.time()

    shiptype = row["Ship type"] if row["Ship type"] else [None]

    ship_type_dimension_id = ship_type_dimension.ensure(
        {'ship_type': shiptype}
    )

    type_of_position_fixing_device_dimension_id = type_of_position_fixing_device_dimension.ensure(
        {'device_type': row["Type of position fixing device"]}
    )

    cargo_type_dimension_id = cargo_type_dimension.ensure(
        {'cargo_type': row["Cargo type"]}
    )

    navigational_status_dimension_id = navigational_status_dimension.ensure(
        {'navigational_status': row["Navigational status"]}
    )

    type_of_mobile_dimension_id = type_of_mobile_dimension.ensure(
        {'mobile_type': row["Type of mobile"]}
    )

    destination_dimension_id = destination_dimension.ensure(
        {'user_defined_destination': row['Destination'],
        'mapped_destination': row['Destination']}
    )

    data_source_type_dimension_id = data_source_type_dimension.ensure(
        {'data_source_type': row["Data source type"]}
    )

    t2 = time.time()

    rot = row["ROT"]
    if(rot == ""):
        rot = None
    sog = row["SOG"] if row["SOG"] != "" else None
    cog = row["COG"] if row["COG"] != "" else None
    heading = row["Heading"] if row["Heading"] != "" else 0
    coordinateString = ("POINT(" + row["Longitude"] + " " + row["Latitude"] + ")")

    fact_table.insert(
        {'eta_date_id': convertTimestampToDateId(row["ETA"]),
        'eta_time_id': convertTimestampToTimeId(row["ETA"]),
        'ship_id': ship_dimension_id,
        'ts_date_id': convertTimestampToDateId(row["# Timestamp"]),
        'ts_time_id': convertTimestampToTimeId(row["# Timestamp"]),
        'data_source_type_id': data_source_type_dimension_id,
        'destination_id': destination_dimension_id,
        'type_of_mobile_id': type_of_mobile_dimension_id,
        'navigational_status_id': navigational_status_dimension_id,
        'cargo_type_id': cargo_type_dimension_id,
        'type_of_position_fixing_device_id': type_of_position_fixing_device_dimension_id,
        'ship_type_id': ship_type_dimension_id,
        'coordinate': coordinateString,
        'rot': rot,
        'sog': sog,
        'cog': sog,
        'heading': heading}
    )

    t3 = time.time()

    

    # fact_table.insert({'cargo_type_id':cargoid})

# Python function needed to split the timestamp into its three parts
# def split_timestamp(row):
#     """Splits a timestamp containing a date into its three parts"""

#     # Splitting of the timestamp into parts
#     timestamp = row['timestamp']
#     row['year'] = timestamp.year
#     row['month'] = timestamp.month
#     row['day'] = timestamp.day

# # The location dimension is loaded from the CSV file, and in order for
# # the data to be present in the database, the shared connection is asked
# # to commit
# [location_dimension.insert(row) for row in ais_source]

# # The file handle for the CSV file can then be closed

# ais_file_handle.close()

# # Each row in the sales database is iterated through and inserted
# for row in sales_source:

#     # Each row is passed to the timestamp split function for splitting
#     split_timestamp(row)

#     # Lookups are performed to find the key in each dimension for the fact
#     # and if the data is not there, it is inserted from the sales row
#     row['bookid'] = book_dimension.ensure(row)
#     row['timeid'] = time_dimension.ensure(row)

#     # The location dimension is pre-filled, so a missing row is an error
#     row['locationid'] = location_dimension.lookup(row)
#     if not row['locationid']:
#         raise ValueError("city was not present in the location dimension")

#     # The row can then be inserted into the fact table
#     fact_table.insert(row)

# # The data warehouse connection is then ordered to commit and close
dw_conn_wrapper.commit()
dw_conn_wrapper.close()

print(datetime.now())