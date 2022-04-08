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

    all_ships = pd.DataFrame()
    all_ships = ships_to_dataframe(connection)
    print("test")

    #top should be called bottom 
    top_5_procent = all_ships.quantile(q=.05, axis=0)["max_draught"]
    top_95_procent = all_ships.quantile(q=.95, axis=0)["max_draught"]

    top_5_procent_relation = all_ships.quantile(q=.05, axis=0)["relation"]
    top_95_procent_relation = all_ships.quantile(q=.95, axis=0)["relation"]
    print(top_5_procent)

    # for i, ship in all_ships.iterrows():
    #     trust_result = score_for_one_ship(ship)
    #     update_database(ship["mmsi"], trust_result)

    return 0 





def ships_to_dataframe(connection):

    query_all_ships = f''' 
            SELECT dim_ship.ship_id, max(draught) as max_draught, ((width+length)/max(draught)) as relation, mmsi, name, width, length, dim_ship.ship_type_id, dim_ship.type_of_position_fixing_device_id, dim_ship.type_of_mobile_id, trust_id
            FROM fact_ais
            inner join dim_ship
            on fact_ais.ship_id = dim_ship.ship_id
            group by dim_ship.ship_id
            limit(10000)
            '''

    result_df = pd.DataFrame(SQLSource(connection=connection, query=query_all_ships))
    
    return result_df


def score_for_one_ship(ship):
    result = 0.0
    type_of_pos_bad = [1,3]
    type_of_ship_bad = [1,5,16]

    if(ship['type_of_mobile_id'] == 2) :
        result += 0.6

    if(not ship['type_of_position_fixing_device_id'] in type_of_pos_bad):
        result += 0.1
    
    if(not math.isnan(ship["imo"])):
        result += 0.1
    
    if(not (math.isnan(ship['width']) or math.isnan(ship["length"]))):
        result += 0.1

    if(not ship['ship_type_id'] in type_of_ship_bad):
        result += 0.1 

    

    result = round(result, 1)
    return result



def update_database(mmsi, result):
    #connection something 

    print(str(mmsi) + " , " + str(result))
    
    query_update_one_ship = f''' 
        update public.dim_ship
        set trust_id = {result}
        where mmsi = {mmsi}
    
    '''
    

