import psycopg2
import pygrametl
from pygrametl.datasources import SQLSource, CSVSource, ProcessSource, TransformingSource
from pygrametl.tables import BulkFactTable, DecoupledFactTable, DimensionPartitioner, DecoupledDimension, Dimension, CachedDimension, FactTable, SlowlyChangingDimension
from datetime import datetime
import configparser
from pygrametl.parallel import shareconnectionwrapper, getsharedsequencefactory
from pygrametl import ConnectionWrapper
from datetime import datetime
import configparser
# from helper_functions import create_tables

BATCHSIZE = 500
connection = None
FILE_PATH = "/home/ubuntu/data/aisdk-2021-09-05.csv"

fileObj = open(FILE_PATH, 'r')

def pgbulkloader(name, attributes, fieldsep, rowsep, nullval, filename):
    filehandle = open(filename, 'r')
    global connection
    cursor = connection.cursor()
    cursor.copy_from(file=filehandle, table=name, sep=fieldsep, null=str(nullval),
                     columns=attributes)

dw_string = "host='localhost' dbname='p9-test-3' user='postgres' password='admin'"

connection = psycopg2.connect(dw_string)
dw_conn_wrapper = ConnectionWrapper(connection=connection)
scw = shareconnectionwrapper(dw_conn_wrapper, 15, (pgbulkloader,))

# psycopg initialization

# commands = create_tables()

# try:
#     cur = connection.cursor()
#     for command in commands:
#         cur.execute(command)
#         # close communication with the PostgreSQL database server
#     # cur.close()
#         # commit the changes
#     connection.commit()
# except (Exception, psycopg2.DatabaseError) as error:
#         print(error)
# finally:
#     if connection is not None:
#         connection.close()

# Function to extract proper id from a timestamp


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


# Creation of dimension and fact table abstractions for use in the ETL flow
date_dimension = CachedDimension(
    name='date',
    key='date_id',  # Lav den til en smartkey.
    attributes=['millennium', 'century', 'decade', 'iso_year', 'year', 'month', 'day',
                'day_of_week', 'iso_day_of_week', 'day_of_year', 'quarter', 'epoch', 'week'],
    prefill=True,
    cacheoninsert=True,
    targetconnection=scw.copy()
)

time_dimension = CachedDimension(
    name='time',
    key='time_id',  # Lav den til en smartkey.
    attributes=['hour', 'minute', 'second'],
    prefill=True,
    cacheoninsert=True,
    targetconnection=scw.copy()
)

ship_type_dimension = CachedDimension(
    name='ship_type',
    key="ship_type_id",
    attributes=['ship_type'],
    prefill=True,
    cacheoninsert=True,
    targetconnection=scw.copy()
)

type_of_position_fixing_device_dimension = CachedDimension(
    name='type_of_position_fixing_device',
    key='type_of_position_fixing_device_id',
    attributes=['device_type'],
    prefill=True,
    cacheoninsert=True,
    targetconnection=scw.copy()
)

cargo_type_dimension = CachedDimension(
    name='cargo_type',
    key='cargo_type_id',
    attributes=['cargo_type'],
    prefill=True,
    cacheoninsert=True,
    targetconnection=scw.copy()
)

navigational_status_dimension = CachedDimension(
    name='navigational_status',
    key='navigational_status_id',
    attributes=['navigational_status'],
    prefill=True,
    cacheoninsert=True,
    targetconnection=scw.copy()
)

type_of_mobile_dimension = CachedDimension(
    name='type_of_mobile',
    key='type_of_mobile_id',
    attributes=['mobile_type'],
    prefill=True,
    cacheoninsert=True,
    targetconnection=scw.copy()
)


destination_dimension = CachedDimension(
    name='destination',
    key='destination_id',
    attributes=['user_defined_destination', 'mapped_destination'],
    prefill=True,
    cacheoninsert=True,
    targetconnection=scw.copy()
)

data_source_type_dimension = CachedDimension(
    name='data_source_type',
    key='data_source_type_id',
    attributes=['data_source_type'],
    prefill=True,
    cacheoninsert=True,
    targetconnection=scw.copy()
)


def getshipdiminstances():
    global scw
    idfactory = getsharedsequencefactory(0)
    for i in range(2):
        yield DecoupledDimension(
            CachedDimension(
                name='ship',
                key='ship_id',
                attributes=['MMSI', 'IMO', 'Name', 'Width', 'Length', 'Callsign',
                            'Draught', 'size_a', 'size_b', 'size_c', 'size_d'],
                size=0,
                cachefullrows=True,
                idfinder=idfactory(),
                targetconnection=scw.copy(),
            ),
            batchsize=BATCHSIZE,
            returnvalues=True,
        )

shipdim = DimensionPartitioner([sd for sd in getshipdiminstances()])

fact_table = DecoupledFactTable(
    BulkFactTable(
        name='fact_table',
        keyrefs=['eta_date_id', 'eta_time_id', 'ship_id', 'ts_date_id', 'ts_time_id', 'data_source_type_id', 'destination_id',
                 'type_of_mobile_id', 'navigational_status_id', 'cargo_type_id', 'type_of_position_fixing_device_id', 'ship_type_id'],
        measures=['coordinate', 'rot', 'sog', 'cog', 'heading', ],
        fieldsep=',',
        rowsep='\\r\n',
        nullsubst=str(None),
        bulkloader=scw.copy().pgbulkloader,
        usefilename=True,
    ),
    batchsize=BATCHSIZE,
    consumes=shipdim.parts,
)

print(datetime.now())

def validateToNull(val): return None if (val == "" or val == "Unknown") else val
def validateToZero(val): return 0 if (val is None or val == "" or val == "Unknown") else val

def transformNulls(row):
    for value in row:
        val = row[value]
        row[value] = validateToNull(val)

ais_source = CSVSource(f=fileObj, delimiter=',')

transformeddata = TransformingSource(ais_source, transformNulls)

inputdata = ProcessSource(transformeddata, batchsize=BATCHSIZE, queuesize=10)

i = 0
for row in inputdata:
    i = i + 1
    if (i % 10000 == 0):
        print(str(datetime.now()) + " Reached milestone: " + str(i))

    fact = {}

    fact["ship_id"] = shipdim.ensure(row, {
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


dw_conn_wrapper.commit()
dw_conn_wrapper.close()

print(datetime.now())
