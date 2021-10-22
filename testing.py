from datetime import datetime
from helper_functions import create_audit_dimension, create_tables, create_trajectory_fact_table
from pygrametl.datasources import SQLSource
from pygrametl.tables import FactTable
from pygrametl.datasources import SQLSource, CSVSource
from pygrametl.tables import Dimension, FactTable
import pygrametl
import configparser
import psycopg2
import numpy as np 
import pandas as pd
import folium 
from folium import plugins
from folium.plugins import HeatMap
from sktime.transformations.series.outlier_detection import HampelFilter

config = configparser.ConfigParser()
config.read('application.properties')

connection_string = "host='localhost' dbname='{}' user='{}' password='{}'".format(
    config["Database"]["dbname"],
    config["Database"]["dbuser"],
    config["Database"]["dbpass"]
)

connection = psycopg2.connect(connection_string)

## TODO: Temporary lists for testing purposes
relevant_ship_mmsi = [219026147, 219027309, 219028440, 257201000, 211188000, 357562000, 373054000, 577097000]
relevant_ship_ids = [649, 696, 3916, 5666] # Notice that some of the ship MMSI's was not found in my dataset

def generate_ship_query_condition():
    string = ''
    for i, ship_id in enumerate(relevant_ship_ids):
        if (i + 1) == len(relevant_ship_ids):
            string += '"ship_id" = ' + str(ship_id)
            return string
        string += '"ship_id" = ' + str(ship_id) + ' OR '

def generate_trajectories(s_data):
    # s_data = Series [{ ship_id , lat , long } , ...]
    return []

cur = connection.cursor()

fetch_ship_data_query = """
    SELECT ship_id, ST_X(coordinate::geometry) as long, ST_Y(coordinate::geometry) as lat from fact_ais 
    WHERE {}
    ORDER BY ship_id, ts_date_id, ts_time_id ASC
""".format(generate_ship_query_condition())

cur.execute(fetch_ship_data_query)

# Data retrieval
print("Retrieving data from db")
data = pd.DataFrame(cur.fetchall(), columns=['ship_id', 'long' , 'lat'])
print("Data received - now starting group by")
grouped_data = data.groupby('ship_id')

print(len(grouped_data))

for ship in grouped_data.groups:
    ship_data = grouped_data.get_group(ship)
    trajectories = generate_trajectories(ship_data)

# h_filter = HampelFilter(window_length=10)

# filtered_data = {}

# def generate_filtered_points(aShip):
#     result_lat = h_filter.fit_transform(aShip['lat'].reset_index(drop=True))
#     result_long = h_filter.fit_transform(aShip['long'].reset_index(drop=True))

#     return pd.concat([result_lat, result_long], axis=1)

# print("Starting filtering")
# # Iterate through groups and do hampelfilter
# for ship in grouped_data.groups:
#     print(ship)
#     ship_data = grouped_data.get_group(ship)

#     result = generate_filtered_points(ship_data)
#     filtered_data[ship] = result.copy()