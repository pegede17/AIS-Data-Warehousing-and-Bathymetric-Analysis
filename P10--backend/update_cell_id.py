from math import ceil
from database_connection import connect_to_local, connect_via_ssh
from pygrametl.datasources import SQLSource, CSVSource
import psycopg2


def get_Query_Update(x, y):
    return f"""

    update fact_ais_clean
    set cell_id = ((ceil((ST_Y(ST_Transform(coordinate::geometry,3034))-3055475)/50)-1)*15798) + ceil((ST_X(ST_Transform(coordinate::geometry,3034))-3602375)/50)
    where fact_id between {x} and {y};

    commit;

    """



def update_cellID():

    connection = connect_to_local()


    cursor = connection.cursor()

    

    QUERY_COMMIT = f"""
    commit;
    """

    QUERY_COUNT_MAX = f"""
    SELECT fact_id from fact_ais_clean
    order by fact_id desc 
    limit 1;
    """

    QUERY_COUNT_MIN = f"""
    SELECT fact_id from fact_ais_clean
    order by fact_id asc 
    limit 1;
    """

    cursor.execute(QUERY_COUNT_MAX)
    endNumber = cursor.fetchone()[0]


    cursor.execute(QUERY_COUNT_MIN)
    startNumber = cursor.fetchone()[0]

    x = endNumber - 10000
    y = endNumber 

    iterrations = ceil((endNumber - startNumber)/10000)

    for i in range(iterrations):

        cursor.execute(get_Query_Update(x,y))


        if (i % 100 == 0):
            print("reached milestone " + str(i) + " out of " + str(iterrations)) 

        x = x - 10000
        y = y - 10000

    
update_cellID()