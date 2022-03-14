from time import perf_counter
import pygrametl
from database_connection import connect_to_local, connect_via_ssh
from pygrametl.datasources import SQLSource, CSVSource
import configparser
import pandas as pd
import math 
import pyperclip
from datetime import datetime, timedelta

def degreesToRadians(degrees):
    return degrees * math.pi / 180


def distanceInKmBetweenEarthCoordinates(lat1, lon1, lat2, lon2):
    earthRadiusKm = 6371

    dLat = degreesToRadians(lat2-lat1)
    dLon = degreesToRadians(lon2-lon1)

    lat1 = degreesToRadians(lat1)
    lat2 = degreesToRadians(lat2)
    a = math.sin(dLat/2) * math.sin(dLat/2) + math.sin(dLon/2) * math.sin(dLon/2) * math.cos(lat1) * math.cos(lat2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return earthRadiusKm * c

def traj_splitter(journey, speedTreshold, timeThreshold, SOGLimit):
    firstPointWithLowSpeed = -1
    ship_trajectories = []
    ship_stops = []
    tempTrajectoryList = []
    tempStopList = []
    timeSinceAboveThreshold = timedelta(minutes=0)
    lastPointOverThreshold = 0


    for i, point in enumerate(journey):

        # journey[i]['time'] = datetime.strptime(str(journey[i]['ts_time_id']), "%H%M%S")
        journey[i]['time'] = datetime(year=1, month=1, day=1, hour=journey[i]['hour'], minute=journey[i]['minute'], second=journey[i]['second'])
        previousPoint = journey[i-1]
        distanceToLastPoint = distanceInKmBetweenEarthCoordinates(point['lat'],point['long'], journey[i-1]['lat'], journey[i-1]['long']) * 1000
        
        # Determine time since last point or set to 0 if it is the first point
        if(i != 0):
            if ((point['lat'] == previousPoint['lat'] and point['long'] == previousPoint['long']) or point['time'] == previousPoint['time']):
                continue
            timeSinceLastPoint = point['time'] - journey[i-1]['time']
        else:
            timeSinceLastPoint = timedelta(0)
        
        #Set speed depending on if time has passed since last point/if we have a previous point
        if(timeSinceLastPoint.seconds != 0):
            calculatedSpeed = distanceToLastPoint/timeSinceLastPoint.seconds
            speed = calculatedSpeed
        else:
            speed = float(point['sog'])

        # Split trajectories if it has been too long since last point
        if(timeSinceLastPoint > timedelta(minutes=5)):
            # End current trajectory session and push to trajectory list
            if(len(tempTrajectoryList) > 0):
                if(firstPointWithLowSpeed != -1):
                    tempTrajectoryList = tempTrajectoryList + journey[firstPointWithLowSpeed:i - 1]
                    firstPointWithLowSpeed = -1
                ship_trajectories.append(tempTrajectoryList.copy())
                tempTrajectoryList.clear()
                
            elif(len(tempStopList) > 0):
                if(firstPointWithLowSpeed != -1):
                    tempStopList = tempStopList + journey[firstPointWithLowSpeed:i - 1]
                    firstPointWithLowSpeed = -1
                ship_stops.append(tempStopList.copy())
                tempStopList.clear()
            speed = float(point['sog'])

        # Skip a point if it is has a too high speed/it is an outlier
        if(speed > SOGLimit):
            print(str(point['long']) + ", " + str(point['lat']))
            if(firstPointWithLowSpeed == -1):
                continue
            if(len(tempStopList) > 0):
                tempStopList = tempStopList + journey[firstPointWithLowSpeed:i - 1]
            elif (len(tempTrajectoryList) > 0):
                tempTrajectoryList = tempTrajectoryList + journey[firstPointWithLowSpeed:i - 1]
            firstPointWithLowSpeed = -1
            continue

        if(speed < speedTreshold):
            # Keep track of the first point with a low speed
            if(firstPointWithLowSpeed == -1):
                firstPointWithLowSpeed = i
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
                if(firstPointWithLowSpeed != -1 and firstPointWithLowSpeed != i):
                    test = journey[firstPointWithLowSpeed:i+1]
                    tempStopList = tempStopList + journey[firstPointWithLowSpeed:i+1]
                else:
                    tempStopList = tempStopList + journey[firstPointWithLowSpeed:i+1]
                firstPointWithLowSpeed = -1

        if(speed >= speedTreshold):
            # Reset counters
            lastPointOverThreshold = i
            # End a "stopped" session and push to stops list
            if(len(tempStopList) > 0):
                ship_stops.append(tempStopList.copy())
                tempStopList.clear()
            # Add points to current trajectory
            if(firstPointWithLowSpeed != -1):
                tempTrajectoryList = tempTrajectoryList + journey[firstPointWithLowSpeed:i+1]
            else:
                tempTrajectoryList.append(point)
            timeSinceAboveThreshold = timedelta(minutes=0)
            firstPointWithLowSpeed = -1


    if(len(tempStopList) > 0):
        ship_stops.append(tempStopList.copy())
        tempStopList.clear()
    if(len(tempTrajectoryList) > 0):
        ship_trajectories.append(tempTrajectoryList.copy())
        tempTrajectoryList.clear()
    firstpoint = True

    linestring = "GEOMETRYCOLLECTION("
    for traj in ship_trajectories:
        linestring = linestring + "LINESTRING("
        firstpoint = True
        for point in traj:
            if (not firstpoint):
                linestring = linestring + ","
            linestring = linestring + str(point['long'])
            linestring = linestring + " "
            linestring = linestring + str(point['lat'])
            firstpoint = False
        linestring = linestring + "),"
    linestring = linestring + ")"

    pyperclip.copy(linestring)
    print("test")


# gets all AIS data from a given day, to create a journey - then calls splitter - and sets trajectories into database 
def create_trajectories(date_to_lookup, config):
    #sets connetction 
    if(config["Environment"]["development"] == "True"):
        connection = connect_via_ssh()
    else:
        connection = connect_to_local()
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

    #query to select all AIS points from the given day
    query_get_all_ais_from_date = f''' 
        SELECT ship_type_id, ts_date_id, ship_id, ts_time_id, audit_id, ST_X(coordinate::geometry) as long, ST_Y(coordinate::geometry) as lat, sog, hour, minute, second, draught
        FROM fact_ais_clean_v2
        INNER JOIN dim_time ON dim_time.time_id = ts_time_id
        WHERE ts_date_id = {date_to_lookup}
        ORDER BY ship_id, ts_time_id ASC
        '''
    
    
    t_query_execution_start = perf_counter()
    
    #translate query to groupby dataframe on ship id   
    all_journeys_as_dataframe = (pd.DataFrame(SQLSource(connection=connection, query=query_get_all_ais_from_date))
                                .groupby(['ship_id']))
    
    






listOfShips = []
listOfPointsInShip = []
ship_id = -1
for element in qr_cleaned_data:
    if (element['ship_id'] == ship_id):
        listOfPointsInShip.append(element)
    else:
        if(len(listOfPointsInShip) != 0):
            listOfShips.append(listOfPointsInShip.copy())
            listOfPointsInShip.clear()

        ship_id = element['ship_id']
        listOfPointsInShip.append(element)
listOfShips.append(listOfPointsInShip)

for ship in listOfShips:
    traj_splitter(ship, speedTreshold=0.5, timeThreshold=300, SOGLimit=200)
