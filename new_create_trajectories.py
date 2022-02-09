from time import perf_counter
import pygrametl
from database_connection import connect_to_local, connect_via_ssh
from pygrametl.datasources import SQLSource, CSVSource
import configparser
import pandas as pd

from datetime import datetime, timedelta

def traj_splitter(journey, speedTreshold, timeThreshold, SOGLimit):
    firstPointNotAdded = -1
    ship_trajectories = []
    ship_stops = []
    tempTrajectoryList = []
    tempStopList = []
    timeSinceAboveThreshold = timedelta(minutes=0)
    lastPointOverThreshold = 0

    for i in range(len(journey)):

        point = journey[i]

        if(float(point['sog']) < speedTreshold):
            # Keep track of the first point with a low speed
            if(firstPointNotAdded == -1):
                firstPointNotAdded = i
            # Update time since above threshold
            # timeSinceAboveThreshold = (datetime.strptime(point['timestamp'], '%d-%m-%Y, %H:%M:%S') - datetime.strptime(journey[lastPointOverThreshold]['timestamp'],  '%d-%m-%Y, %H:%M:%S') )
            timeSinceAboveThreshold = point['ts_time_id'] - journey[lastPointOverThreshold]['ts_time_id'] 
            
            if(timeSinceAboveThreshold <= timeThreshold):
                continue

            if(timeSinceAboveThreshold > timeThreshold):
                # End "sailing session and push to trajectory list
                if(len(tempTrajectoryList) > 0):
                    ship_trajectories.append(tempTrajectoryList.copy())
                    tempTrajectoryList.clear()
                # Add points to current stop session
                if(firstPointNotAdded != -1 and firstPointNotAdded != i):
                    test = journey[firstPointNotAdded:i+1]
                    tempStopList = tempStopList + journey[firstPointNotAdded:i+1]
                else:
                    tempStopList = tempStopList + journey[firstPointNotAdded:i+1]
                firstPointNotAdded = -1

        if(float(point['sog']) >= speedTreshold):
            # Reset counters
            lastPointOverThreshold = i
            # End a "stopped" session and push to stops list
            if(len(tempStopList) > 0):
                ship_stops.append(tempStopList.copy())
                tempStopList.clear()
            # Add points to current trajectory
            if(firstPointNotAdded != -1):
                tempTrajectoryList = tempTrajectoryList + journey[firstPointNotAdded:i+1]
            else:
                tempTrajectoryList.append(point)
            timeSinceAboveThreshold = timedelta(minutes=0)
            firstPointNotAdded = -1


    if(len(tempStopList) > 0):
        ship_stops.append(tempStopList.copy())
        tempStopList.clear()
    if(len(tempTrajectoryList) > 0):
        ship_trajectories.append(tempTrajectoryList.copy())
        tempTrajectoryList.clear()

    firstpoint = True
    print("GEOMETRYCOLLECTION(", end ="")
    for traj in ship_trajectories:
        print("LINESTRING(", end =""),
        firstpoint = True
        for point in traj:
            if (not firstpoint):
                print(",", end ="")
            print(str(point['long']), end =""),
            print(" ", end =""),
            print(str(point['lat']), end =""),
            firstpoint = False
        print("),", end ="")
    print(")")

config = configparser.ConfigParser()
config.read('application.properties')

if(config["Environment"]["development"] == "True"):
    connection = connect_via_ssh()
else:
    connection = connect_to_local()
dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

# Queries defined
query = """
SELECT ship_type_id, ts_date_id, ship_id, ts_time_id, audit_id, ST_X(coordinate::geometry) as long, ST_Y(coordinate::geometry) as lat, sog, hour, minute, second, draught
FROM fact_ais_clean_v2
INNER JOIN dim_time ON dim_time.time_id = ts_time_id
WHERE ts_date_id = 20210110 AND ship_id = 10
"""

date_query = """
SELECT year, month, day
FROM dim_date
where date_id = {}
""".format(20210110)

t_query_execution_start = perf_counter()

qr_cleaned_data = SQLSource(connection=connection, query=query)
qr_date_details = SQLSource(connection=connection, query=date_query)


list = []
for element in qr_cleaned_data:
    list.append(element)


# data_trajectories = pd.DataFrame(qr_cleaned_data)

traj_splitter(list, 1, 5, 100)
