import psycopg2
import pygrametl
from pygrametl.datasources import SQLSource, CSVSource
from pygrametl.tables import Dimension, FactTable, SlowlyChangingDimension
from helper_functions import create_tables
from datetime import datetime



dw_pgconn = None

dw_string = "host='localhost' dbname='p9-test' user='postgres' password='admin'"
dw_pgconn = psycopg2.connect(dw_string)
dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=dw_pgconn)

ais_file_handle = open('aisdk-2021-07/aisdk-2021-07-31.csv', 'r')
ais_source = CSVSource(f=ais_file_handle, delimiter=',')

# psycopg initialization

# commands = create_tables()

# try:
#     cur = dw_pgconn.cursor()
#     for command in commands:
#         cur.execute(command)
#         # close communication with the PostgreSQL database server
#     cur.close()
#         # commit the changes
#     dw_pgconn.commit()
# except (Exception, psycopg2.DatabaseError) as error:
#         print(error)
# finally:
#     if dw_pgconn is not None:
#         dw_pgconn.close()


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

# Creation of dimension and fact table abstractions for use in the ETL flow
date_dimension = Dimension(
    name='date',
    key='date_id', #Lav den til en smartkey.
    attributes=['millennium', 'century', 'decade', 'iso_year', 'year', 'month', 'day', 'day_of_week', 'iso_day_of_week', 'day_of_year', 'quarter', 'epoch', 'week'])

time_dimension = Dimension(
    name='time',
    key='time_id', #Lav den til en smartkey.
    attributes=['hour', 'minute', 'second']
)

ship_dimension = Dimension(
    name='ship',
    key='ship_id',
    attributes=['mmsi', 'imo', 'name', 'width', 'length', 'callsign', 'draught', 'size_a', 'size_b', 'size_c', 'size_d'],
)

ship_type_dimension = Dimension(
    name='ship_type',
    key="ship_type_id",
    attributes=['ship_type']
)

type_of_position_fixing_device_dimension = Dimension(
    name='type_of_position_fixing_device',
    key='type_of_position_fixing_device_id',
    attributes=['device_type']
)

cargo_type_dimension = Dimension(
    name='cargo_type',
    key='cargo_type_id',
    attributes=['cargo_type']
)

navigational_status_dimension = Dimension(
    name='navigational_status',
    key='navigational_status_id',
    attributes=['navigational_status']
)

type_of_mobile_dimension = Dimension(
    name='type_of_mobile',
    key='type_of_mobile_id',
    attributes=['mobile_type']
)

destination_dimension = Dimension(
    name='destination',
    key='destination_id',
    attributes=['user_defined_destination', 'mapped_destination']
)

data_source_type_dimension = Dimension(
    name='data_source_type',
    key='data_source_type_id',
    attributes=['data_source_type']
)

fact_table = FactTable(
    name='fact_table',
    keyrefs=['eta_date_id', 'eta_time_id', 'ship_id', 'ts_date_id', 'ts_time_id','data_source_type_id', 'destination_id', 'type_of_mobile_id', 'navigational_status_id', 'cargo_type_id', 'type_of_position_fixing_device_id', 'ship_type_id'],
    measures=['coordinate', 'rot', 'sog', 'cog', 'heading', ]
)

i = 0
for row in ais_source:
    i = i + 1
    if (i == 100000):
        break
    
    imo = row["IMO"] if row["IMO"] != "Unknown" else None
    name = row["Name"] if row["Name"] != "" else [None]
    width = row["Width"] if row["Width"] != "" else None
    length = row["Length"] if row["Length"] != "" else None
    callsign =  row["Callsign"] if row["Callsign"] != "" else [None]
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

    rot = row["ROT"] if row["ROT"] != "" else None
    sog = row["SOG"] if row["SOG"] != "" else None
    cog = row["COG"] if row["COG"] != "" else None
    heading = row["Heading"] if row["Heading"] != "" else None

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
        'coordinate': 'POINT(-118.4079 33.9434)',
        'rot': rot,
        'sog': sog,
        'cog': sog,
        'heading': heading}
    )

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

