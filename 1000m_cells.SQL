CREATE TABLE dim_cell_3034_1000m(
	cell_id SERIAL PRIMARY KEY,
	columnx INTEGER,
	rowy INTEGER,
	boundary GEOMETRY
)

CREATE TABLE bridge_traj_sailing_cell_3034_1000m(
	cell_id INTEGER NOT NULL,
	trajectory_id BIGINT NOT NULL,
          
	FOREIGN KEY (cell_id)
		REFERENCES dim_cell_3034_1000m (cell_id)
		ON UPDATE CASCADE,
	FOREIGN KEY (trajectory_id)
		REFERENCES fact_trajectory_sailing (trajectory_id)
		ON UPDATE CASCADE,
	PRIMARY KEY (cell_id, trajectory_id)
)

With start_point as (
	SELECT 3602375 x_start, 3055475 y_start
)
INSERT INTO dim_cell_3034_1000m (columnx, rowy, boundary) 
	SELECT columnx, rowy,
	ST_MakeEnvelope(x_start + ((columnx - 1) * 1000),y_start + ((rowy - 1) * 1000),x_start + (columnx * 1000),y_start + (rowy * 1000),3034)
	FROM generate_series(1, 790) columnx, generate_series(1, 417) rowy, start_point



SELECT DISTINCT ON (columnx_1000m, rowy_1000m) cell_id, columnx_1000m, rowy_1000m, boundary_1000m from dim_cell_3034 limit 10

SELECT ST_Transform(boundary,4326) from dim_cell_3034_1000m ORDER BY cell_id desc limit 10000