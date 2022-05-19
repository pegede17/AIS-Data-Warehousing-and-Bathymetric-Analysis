CREATE TABLE IF NOT EXISTS fact_cell_3034_50m(
            fact_cell_id BIGSERIAL NOT NULL PRIMARY KEY,
            date_id INTEGER NOT NULL,
            cell_id BIGINT NOT NULL,
            ship_type_id INTEGER NOT NULL,
            type_of_mobile_id INTEGER NOT NULL,
            trust_id BOOLEAN DEFAULT False,
            audit_id INTEGER NOT NULL,
            trajectory_count INTEGER NOT NULL,
            max_draught DOUBLE PRECISION,
            min_draught DOUBLE PRECISION,
            avg_draught DOUBLE PRECISION,
            min_traj_speed DOUBLE PRECISION,
            max_traj_speed DOUBLE PRECISION,
            avg_traj_speed DOUBLE PRECISION,
            histogram_draught INTEGER[256],
            histogram_traj_speed INTEGER[1024],

            FOREIGN KEY(date_id)
                REFERENCES dim_date (date_id)
                ON UPDATE CASCADE,
            FOREIGN KEY(cell_id)
                REFERENCES dim_cell (cell_id)
                ON UPDATE CASCADE,
            FOREIGN KEY(ship_type_id)
                REFERENCES dim_ship_type (ship_type_id)
                ON UPDATE CASCADE,
            FOREIGN KEY(type_of_mobile_id)
                REFERENCES dim_type_of_mobile (type_of_mobile_id)
                ON UPDATE CASCADE
        );

ALTER TABLE fact_cell_3034_50m DISABLE TRIGGER ALL;
INSERT INTO fact_cell_3034_50m(date_id, cell_id, ship_type_id, type_of_mobile_id, is_draught_trusted, audit_id, trajectory_count, max_draught, min_draught, avg_draught, min_traj_speed, max_traj_speed, avg_traj_speed, histogram_draught, histogram_traj_speed) SELECT date_start_id,
		dim_cell.cell_id, 
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
inner join dim_cell on dim_cell.cell_id = b.cell_id
WHERE date_start_id > 20210501
GROUP BY date_start_id, dim_cell.cell_id, ship_type_id, type_of_mobile_id, is_draught_trusted;
ALTER TABLE fact_cell_3034_50m ENABLE TRIGGER ALL;
CREATE INDEX ON fact_cell_3034_50m (cell_id);
CREATE INDEX ON fact_cell_3034_50m (date_id, ship_type_id, type_of_mobile_id, is_draught_trusted);
VACUUM ANALYZE fact_cell_3034_50m;