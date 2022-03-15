import configparser
from datetime import date, datetime, timedelta
from math import ceil
from time import perf_counter
# from helper_functions import create_audit_dimension
from database_connection import connect_to_local, connect_via_ssh
from pygrametl.datasources import SQLSource
import pygrametl
import rasterio
from pyproj import Transformer

# for animal in dfGroup:
#     for a, element in animal:
#         print(element)
config = configparser.ConfigParser()
config.read('P10--backend/application.properties')
if(config["Environment"]["development"] == "True"):
    connection = connect_via_ssh()
else:
    connection = connect_to_local()
dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)

query = f''' 
        SELECT DISTINCT ON (eta_date_id, eta_time_id, ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, 
                    navigational_status_id, cargo_type_id, coordinate, draught, rot, sog, cog, heading)
            longitude, latitude,mmsi, fact_ais.type_of_mobile_id, fact_id, eta_date_id, eta_time_id, fact_ais.ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, navigational_status_id, cargo_type_id, coordinate, draught, rot, sog, cog, heading
        FROM fact_ais INNER JOIN public.dim_ship on fact_ais.ship_id = dim_ship.ship_id, public.danish_waters
        WHERE 
            (draught < 28.5 OR draught IS NULL)
            AND width < 75
            AND length < 488
            AND mmsi > 99999999
            AND mmsi < 1000000000
            AND ST_Contains(geom ,coordinate::geometry)
		LIMIT 100
        '''

coordinates = []
calculated_coordinates = []

cur = connection.cursor()

qr_cleaned_data = SQLSource(connection=connection, query=query)
dataset = rasterio.open('P10--backend/test.tif')

start = perf_counter()
transformer = Transformer.from_crs("epsg:4326", "epsg:32632")

for row in qr_cleaned_data:

    x1, y1 = row['latitude'], row['longitude']

    x2, y2 = transformer.transform(x1, y1)
    print(x2, y2)
    # coordinates.append(dataset.index(row['long'], row['lat']))
    coordinates = ceil((x2 - 0) /
                       50), ceil((y2 - 5900000) / 50)
    calculated_coordinates.append(coordinates)
    # print(str(coordinates) + "long: " +
    #       str(row['long']) + "lat: " + str(row['lat']))

end = perf_counter()


print(str(timedelta(seconds=(end - start))))

# for i in coordinates:
#     print(i)

# dataset.index(4040249.83797688, 3251798.169114993)
