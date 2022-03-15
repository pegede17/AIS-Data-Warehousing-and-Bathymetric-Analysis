from random import randrange
import pandas as pd
from database_connection import connect_to_local, connect_via_ssh
import pygrametl
from pygrametl.datasources import SQLSource, CSVSource

connection = connect_to_local()


query_get_all_ais_from_date = """ 
    SELECT ship_type_id, ts_date_id, ship_id, ts_time_id, audit_id, ST_X(coordinate::geometry) as long, ST_Y(coordinate::geometry) as lat, sog, hour, minute, second, draught
    FROM fact_ais
    INNER JOIN dim_time ON dim_time.time_id = ts_time_id
    WHERE ts_date_id = 20211001
    ORDER BY ship_id, ts_time_id ASC
    limit(10000)
    """


all_journeys_as_dataframe = pd.DataFrame(SQLSource(connection=connection, query=query_get_all_ais_from_date))
all_journeys_group = all_journeys_as_dataframe.groupby('ship_id')
ship2 = all_journeys_group.get_group(2) 


def do_shit(ship):
    print('im doing shit')
    for index ,data in ship.iterrows():
        if(randrange(100) == 50 ):
            print(data['lat'] , data['long'])

for index, ship in all_journeys_group:
    print(index)
    do_shit(ship)


print('test')    
