SELECT ship_type_id, ts_date_id, ship_id, ts_time_id, audit_id, ST_X(ST_Transform(coordinate::geometry,3034)) as long, ST_Y(ST_Transform(coordinate::geometry,3034)) as lat, sog, hour, minute, second, draught
        FROM fact_ais
        INNER JOIN dim_time ON dim_time.time_id = ts_time_id
        WHERE ts_date_id = 20211001
        ORDER BY ship_id, ts_time_id ASC
		LIMIT 100
		
CREATE TABLE raster_50m_test (
	ras raster
);
INSERT INTO raster_50m_test VALUES (ST_AddBand(ST_MakeEmptyRaster(22000,16000,0::float,5900000::float,50::float,50::float,0::float,0::float,32632),'8BUI'::text, 0, null));

SELECT ST_Transform((ST_DumpAsPolygons(ras)).geom,4326) from raster_50m_test union 
SELECT geom from danish_waters

SELECT ST_Transform(ST_PixelAsPolygon(ras, 12071, 6579),4326) from raster_50m_test

SELECT * from fact_ais_clean

TRUNCATE fact_ais_clean

CREATE TABLE dim_cell (
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

EXPLAIN With start_point as (
	SELECT 0 x_start, 5900000 y_start
)
INSERT INTO dim_cell (
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
	FROM generate_series(1, 22000) columnx, generate_series(1, 16000) rowy, start_point;
	
	
CREATE INDEX cell_idx on dim_cell (columnx_50m, rowy_50m);
	