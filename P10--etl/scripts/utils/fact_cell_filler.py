from math import ceil
from utils.database_connection import connect_to_db
import configparser


def fill_fact_cell(date):
    config = configparser.ConfigParser()
    config.read('../application.properties')

    connection = connect_to_db(config)

    cur = connection.cursor()
    cur.execute(f"""
    ALTER TABLE fact_cell_3034_50m DISABLE TRIGGER ALL;
    INSERT INTO fact_cell_3034_50m(date_id, 
                                    cell_id, 
                                    ship_type_id, 
                                    type_of_mobile_id, 
                                    is_draught_trusted, 
                                    audit_id, 
                                    trajectory_count, 
                                    max_draught, 
                                    min_draught, 
                                    avg_draught, 
                                    min_traj_speed, 
                                    max_traj_speed, 
                                    avg_traj_speed, 
                                    histogram_draught, 
                                    histogram_traj_speed) 
    SELECT date_start_id,
            dim_cell_3034.cell_id, 
            ship_type_id, 
            type_of_mobile_id,
            is_draught_trusted,
            2 audit_id,
            count(traj.trajectory_id) trajectory_count, 
            MAX(draught[1]) max_draught,
            MIN(draught[1]) min_draught,
            AVG(draught[1]) avg_draught,
            MAX(avg_speed_knots) max_speed,
            MIN(avg_speed_knots) min_speed,
            AVG(avg_speed_knots) avg_speed,
            histogram(CASE WHEN draught[1] IS NOT NULL THEN draught[1] ELSE -0.1 END,-0.1,25,251),
            histogram(avg_speed_knots, 0,100,1000)
    from fact_trajectory_sailing traj 
    inner join bridge_traj_sailing_cell_3034 b 
        on traj.trajectory_id = b.trajectory_id
    inner join dim_cell_3034 on dim_cell_3034.cell_id = b.cell_id
    WHERE date_start_id = {date}
    GROUP BY date_start_id, dim_cell_3034.cell_id, ship_type_id, type_of_mobile_id, is_draught_trusted;
    ALTER TABLE fact_cell_3034_50m ENABLE TRIGGER ALL;
    """)
    connection.commit()
    cur.close()
    connection.close()
