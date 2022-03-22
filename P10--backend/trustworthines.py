#this file should contain the trust worthyness algoritme 

import pandas as pd
import math
import pygrametl
from database_connection import connect_to_local, connect_via_ssh
from pygrametl.datasources import SQLSource, CSVSource
import configparser


def give_trustscore_to_ships(config):

    if(config["Environment"]["development"] == "True"):
        connection = connect_via_ssh()
    else:
        connection = connect_to_local()
    dw_conn_wrapper = pygrametl.ConnectionWrapper(connection=connection)


    #get all ships into a dataframe 
    #for all ships in DF
        #calculate score for single ship 
        #update database for single ship 
    
    return 0 





def ships_to_dataframe(connection):

    query_all_ships = f''' 
            SELECT ship_id, mmsi, imo, name, width, length, callsign, size_a, size_b, size_c, size_d, ship_type_id, type_of_position_fixing_device_id, type_of_mobile_id, trust_id
            FROM dim_ship
            limit(100000)
            '''

    result_df = pd.DataFrame(SQLSource(connection=connection, query=query_all_ships))
    
    return result_df


def score_for_one_ship(ship):
    result = 0.0

    if(ship['type_of_position_fixing_device_id'] == 1) :
        result += 0.5

    return result



def update_database(mmsi, result):
    #connection something 
    
    query_update_one_ship = f''' 
        update public.dim_ship
        set trust_id = {result}
        where mmsi = {mmsi}
    
    '''
    

