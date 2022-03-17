from operator import index
from time import perf_counter
import pygrametl
from database_connection import connect_to_local, connect_via_ssh
from pygrametl.datasources import SQLSource, CSVSource
import configparser
import pandas as pd
import math 
import pyperclip
from datetime import datetime, timedelta

def degrees_to_radians(degrees):
    return degrees * math.pi / 180


def distance_in_km_between_earth_coordinates(lat1, lon1, lat2, lon2):
    earth_radius_km = 6371

    dLat = degrees_to_radians(lat2-lat1)
    dLon = degrees_to_radians(lon2-lon1)

    lat1 = degrees_to_radians(lat1)
    lat2 = degrees_to_radians(lat2)
    a = math.sin(dLat/2) * math.sin(dLat/2) + math.sin(dLon/2) * math.sin(dLon/2) * math.cos(lat1) * math.cos(lat2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return earth_radius_km * c

def traj_splitter(journey:pd.DataFrame, speed_treshold, time_threshold, SOG_limit):
    first_point_with_low_speed = -1
    ship_trajectories = pd.DataFrame()
    ship_stops = pd.DataFrame()
    temp_trajectory_list = pd.DataFrame()
    temp_stop_list = pd.DataFrame()
    time_since_above_threshold = timedelta(minutes=0)
    last_point_over_threshold = 0

    journey = journey.reset_index(0)
    print(journey)

    for i in journey.index:

        # journey.at(i)['time'] = datetime.strptime(str(journey.at(i)['ts_time_id']), "%H%M%S")
        journey.at[i, 'time'] = datetime(year=1, month=1, day=1, hour=journey.at[i, 'hour'], minute=journey.at[i,'minute'], second=journey.at[i,'second'])
        point = journey.iloc[i]

        # Determine time since last point or set to 0 if it is the first point
        if(i != 0):
            previous_point = journey.iloc[i-1]
            if ((point['lat'] == previous_point['lat'] and point['long'] == previous_point['long']) or point['time'] == previous_point['time']):
                continue
            time_since_last_point = point['time'] - previous_point['time']
            distance_to_last_point = distance_in_km_between_earth_coordinates(point['lat'],point['long'], previous_point['lat'], previous_point['long']) * 1000
        else:
            time_since_last_point = timedelta(0)
            distance_to_last_point = 0
        
        #Set speed depending on if time has passed since last point/if we have a previous point
        if(time_since_last_point.seconds != 0):
            calculated_speed = distance_to_last_point/time_since_last_point.seconds
            speed = calculated_speed
        else:
            speed = float(point['sog'])

        # Split trajectories if it has been too long since last point
        if(time_since_last_point > timedelta(minutes=5)):
            # End current trajectory session and push to trajectory list
            if(len(temp_trajectory_list) > 0):
                if(first_point_with_low_speed != -1):
                    temp_trajectory_list = temp_trajectory_list + journey.iloc[first_point_with_low_speed:i - 1]
                    first_point_with_low_speed = -1
                ship_trajectories.append(temp_trajectory_list.copy())
                temp_trajectory_list = temp_trajectory_list.iloc[0:0]
                
            elif(len(temp_stop_list) > 0):
                if(first_point_with_low_speed != -1):
                    temp_stop_list = temp_stop_list + journey.iloc[first_point_with_low_speed:i - 1]
                    first_point_with_low_speed = -1
                ship_stops.append(temp_stop_list.copy())
                temp_stop_list = temp_stop_list.iloc[0:0]
            speed = float(point['sog'])

        # Skip a point if it is has a too high speed/it is an outlier
        if(speed > SOG_limit):
            print(str(point['long']) + ", " + str(point['lat']))
            if(first_point_with_low_speed == -1):
                continue
            if(len(temp_stop_list) > 0):
                temp_stop_list = temp_stop_list + journey.iloc[first_point_with_low_speed:i - 1]
            elif (len(temp_trajectory_list) > 0):
                temp_trajectory_list = temp_trajectory_list + journey.iloc[first_point_with_low_speed:i - 1]
            first_point_with_low_speed = -1
            continue

        if(speed < speed_treshold):
            # Keep track of the first point with a low speed
            if(first_point_with_low_speed == -1):
                first_point_with_low_speed = i
            # Update time since above threshold
            # timeSinceAboveThreshold = (datetime.strptime(point['timestamp'], '%d-%m-%Y, %H:%M:%S') - datetime.strptime(journey[lastPointOverThreshold]['timestamp'],  '%d-%m-%Y, %H:%M:%S') )
            time_since_above_threshold = point['ts_time_id'] - journey.at[last_point_over_threshold, 'ts_time_id'] 
            
            if(time_since_above_threshold <= time_threshold):
                continue

            if(time_since_above_threshold > time_threshold):
                # End "sailing session and push to trajectory list
                if(len(temp_trajectory_list) > 0):
                    ship_trajectories.append(temp_trajectory_list.copy())
                    temp_trajectory_list = temp_trajectory_list.iloc[0:0]
                # Add points to current stop session
                if(first_point_with_low_speed != -1 and first_point_with_low_speed != i):
                    test = journey.iloc[first_point_with_low_speed:i+1]
                    temp_stop_list = temp_stop_list + journey.iloc[first_point_with_low_speed:i+1]
                else:
                    temp_stop_list = temp_stop_list + journey.iloc[first_point_with_low_speed:i+1]
                first_point_with_low_speed = -1

        if(speed >= speed_treshold):
            # Reset counters
            last_point_over_threshold = i
            # End a "stopped" session and push to stops list
            if(len(temp_stop_list) > 0):
                ship_stops.append(temp_stop_list.copy())
                temp_stop_list = temp_stop_list.iloc[0:0]
            # Add points to current trajectory
            if(first_point_with_low_speed != -1):
                temp_trajectory_list = temp_trajectory_list + journey.iloc[first_point_with_low_speed:i+1]
            else:
                temp_trajectory_list.append(point)
            time_since_above_threshold = timedelta(minutes=0)
            first_point_with_low_speed = -1


    if(len(temp_stop_list) > 0):
        ship_stops.append(temp_stop_list.copy())
        temp_stop_list = temp_stop_list.iloc[0:0]
    if(len(temp_trajectory_list) > 0):
        ship_trajectories.append(temp_trajectory_list.copy())
        temp_trajectory_list = temp_trajectory_list.iloc[0:0]
    firstpoint = True

    linestring = "GEOMETRYCOLLECTION("
    for traj in ship_trajectories:
        linestring = linestring + "LINESTRING("
        firstpoint = True
        for point in traj:
            if (not firstpoint):
                linestring = linestring + ","
            linestring = linestring + str(point.long)
            linestring = linestring + " "
            linestring = linestring + str(point.lat)
            firstpoint = False
        linestring = linestring + "),"
    linestring = linestring + ")"

    pyperclip.copy(linestring)
    print("done with one ship")


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
        FROM fact_ais
        INNER JOIN dim_time ON dim_time.time_id = ts_time_id
        WHERE ts_date_id = {date_to_lookup}
        ORDER BY ship_id, ts_time_id ASC
        limit(100000)
        '''
    
     
    t_query_execution_start = perf_counter()
    
    #translate query to groupby dataframe on ship id   
    all_journeys_as_dataframe = (pd.DataFrame(SQLSource(connection=connection, query=query_get_all_ais_from_date ))
                                .groupby(['ship_id']))
    
    

    for i, ship in all_journeys_as_dataframe:
        traj_splitter(ship, speed_treshold=0.5, time_threshold=300, SOG_limit=200)

    
    
    





    # listOfShips = []
    # listOfPointsInShip = []
    # ship_id = -1
    # for element in query_get_all_ais_from_date:
    #     if (element['ship_id'] == ship_id):
    #         listOfPointsInShip.append(element)
    #     else:
    #         if(len(listOfPointsInShip) != 0):
    #             listOfShips.append(listOfPointsInShip.copy())
    #             listOfPointsInShip.clear()

    #         ship_id = element['ship_id']
    #         listOfPointsInShip.append(element)
    # listOfShips.append(listOfPointsInShip)

    # for ship in listOfShips:
    #     traj_splitter(ship, speedTreshold=0.5, timeThreshold=300, SOGLimit=200)
