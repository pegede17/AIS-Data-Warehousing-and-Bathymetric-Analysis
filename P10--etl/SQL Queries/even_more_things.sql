CREATE TABLE dim_cell_test (
	geo_id BIGSERIAL PRIMARY KEY,
	columnx_50m BIGINT,
	rowy_50m BIGINT,
	columnx_100m BIGINT,
	rowy_100m BIGINT,
	columnx_500m BIGINT,
	rowy_500m BIGINT,
	columnx_1000m BIGINT,
	rowy_1000m BIGINT,
	boundary_50m GEOMETRY,
	boundary_100m GEOMETRY,
	boundary_500m GEOMETRY,
	boundary_1000m GEOMETRY
);

With start_point as (
	SELECT 3584734 x_start, 2997812 y_start
)
INSERT INTO dim_cell_test (
	columnx_50m, rowy_50m, 
	columnx_100m, rowy_100m, 
	columnx_500m, rowy_500m, 
	columnx_1000m, rowy_1000m, 
	boundary_50m,
	boundary_100m,
	boundary_500m,
	boundary_1000m) 
	SELECT columnx, rowy,
	ceil(columnx/2.0), ceil(rowy/2.0),
	ceil(columnx/10.0), ceil(rowy/10.0),
	ceil(columnx/20.0), ceil(rowy/20.0),
	ST_MakeEnvelope(x_start + ((columnx - 1) * 50),y_start + ((rowy - 1) * 50),x_start + (columnx * 50),y_start + (rowy * 50),3034),
	ST_MakeEnvelope(x_start + ((ceil(columnx/2.0) - 1) * 100),y_start + ((ceil(rowy/2.0) - 1) * 100),x_start + (ceil(columnx/2.0) * 100),y_start + (ceil(rowy/2.0) * 100),3034),
	ST_MakeEnvelope(x_start + ((ceil(columnx/10.0) - 1) * 500),y_start + ((ceil(rowy/10.0) - 1) * 500),x_start + (ceil(columnx/10.0) * 500),y_start + (ceil(rowy/10.0) * 500),3034),
	ST_MakeEnvelope(x_start + ((ceil(columnx/20.0) - 1) * 1000),y_start + ((ceil(rowy/20.0) - 1) * 1000),x_start + (ceil(columnx/20.0) * 1000),y_start + (ceil(rowy/20.0) * 1000),3034)
	FROM generate_series(1, 1000) columnx, generate_series(1, 1000) rowy, start_point
		
SELECT ST_Equals(f1.geom, f2.geom)
from (SELECT DISTINCT(boundary_50m) geom, columnx_50m, rowy_50m from dim_cell_test) f1, 
				 (SELECT DISTINCT(boundary_50m) geom, columnx_50m, rowy_50m from dim_cell) f2
WHERE f1.rowy_50m = f2.rowy_50m and f1.columnx_50m = f2.columnx_50m

SELECT 

With start_point as (
	SELECT 3584734 x, 2997812 y
)
SELECT ST_Transform(ST_MakeEnvelope(x,y,x+50,y+50,3034),4326)
FROM start_point

INSERT INTO dim_cell_test (boundary_50m) VALUES (LINESTRING(20,20))

SELECT (ST_WorldToRasterCoord(ras, ST_SetSRID(ST_POINT(4040249.83797688, 3251798.169114993),3034))).* from raster_50m

SELECT ST_AsText(ST_Transform(ST_SetSRID(ST_POINT(10.6684, 56.1939),4326),3034))

SELECT ST_AsText(coordinate) from fact_ais limit 10

SELECT 

