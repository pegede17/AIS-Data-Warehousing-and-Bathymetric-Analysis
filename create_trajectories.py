import datetime
from helper_functions import create_tables
import psycopg2
from pygrametl.datasources import SQLSource
from pygrametl.tables import FactTable
from pygrametl.datasources import SQLSource, CSVSource
from pygrametl.tables import BulkFactTable, Dimension, CachedDimension, FactTable, SlowlyChangingDimension
from sshtunnel import SSHTunnelForwarder
import pygrametl
import configparser

# For interactive work (on ipython) it's easier to work with explicit objects
# instead of contexts.

trajectories = []

coordinates = ""

temp_trajectory = {
    'ship_id' : 0, 
    'time_start_id' : 0, 
    'date_start_id' : 0, 
    'time_end_id' : 0, 
    'date_end_id' : 0,
    'coordinates' : "LineString("
}

trajectory_length = 0

current_ship_id = 0


def add_to_trajectory(row):
    global trajectory_length
    trajectory_length = trajectory_length + 1
    temp_trajectory["time_end_id"] = row["ts_time_id"]
    temp_trajectory["date_end_id"] = row["ts_date_id"]
    temp_trajectory['coordinates'] = temp_trajectory['coordinates'] + "," + ((row["coordinate"][6:-1]))

def create_new_trajectory(row):
    global trajectory_length
    trajectory_length = 0
    temp_trajectory["ship_id"] = row["ship_id"]
    temp_trajectory["time_start_id"] = row["ts_time_id"]
    temp_trajectory["date_start_id"] = row["ts_date_id"]
    temp_trajectory["coordinates"] = "LineString(" +  row["coordinate"][6:-1]

def end_trajectory():
    global trajectory_length
    temp_trajectory['coordinates'] = temp_trajectory['coordinates']  + ")"
    if(trajectory_length > 4):
        trajectories.append(temp_trajectory.copy())
    temp_trajectory.clear()



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

query = """
SELECT fact_id, ts_date_id, ship_id, ts_time_id, ST_AsText(coordinate) as coordinate, sog from fact_table 
	ORDER BY ship_id, ts_time_id ASC;
""";

ais_source = SQLSource(connection=connection, query=query)

trajectory_fact_table = FactTable(
    name='trajectory',
    keyrefs=['ship_id', 'time_start_id', 'date_start_id', 'time_end_id', 'date_end_id'],
    measures=['coordinates']
)

i = 0
isCreatingRoute = False

for row in ais_source:
    i = i + 1
    if (i % 100000 == 0):
        print("Reached milestone: " + str(i)) 
        print(datetime.now())
    sog = row["sog"]
    if(sog != 0 and sog != None):
        if(not isCreatingRoute):
            current_ship_id = row["ship_id"]
            create_new_trajectory(row)
            isCreatingRoute = True
        else :
            if(current_ship_id == row["ship_id"]):
                add_to_trajectory(row)
            else:
                end_trajectory()
                create_new_trajectory(row)
    else:
        if(isCreatingRoute):
            end_trajectory()
            isCreatingRoute = False

for traj in trajectories:
    trajectory_fact_table.insert(traj)

dw_conn_wrapper.commit()
dw_conn_wrapper.close()

# # Get a database cursor
# cur = conn.cursor()

# # Execute SQL
# cur.execute("""
# SELECT * FROM "ship";
# """)

# # Get the result
# result = cur.fetchall()
# for row in result:
#     print(row["mmsi"])

# Close connections
connection.close()

# Stop the tunnel
# tunnel.stop()


# SELECT fact_id, ts_date_id, ship_id, ts_time_id, ST_AsText(coordinate) as coordinate, sog from fact_table 
# WHERE  ST_CONTAINS(ST_Polygon('LINESTRING(10.291804 56.994434, 10.318773 56.993728, 10.316578 56.980068, 10.294082 56.982268, 10.291804 56.994434)'::geometry, 4326),coordinate::geometry)
# 	ORDER BY ship_id, ts_time_id ASC;