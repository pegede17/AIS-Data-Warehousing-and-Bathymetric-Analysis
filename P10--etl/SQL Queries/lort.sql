CREATE TABLE raster_50m (
	ras raster
);
INSERT INTO raster_50m VALUES (ST_AddBand(ST_MakeEmptyRaster(20000,20000,3584734::float,2997812::float,50::float,50::float,0::float,0::float,3034),'8BUI'::text, 0, null));


CREATE TABLE raster_100m (
	ras raster
);
INSERT INTO raster_100m VALUES (ST_AddBand(ST_MakeEmptyRaster(10000,10000,3584734::float,2997812::float,100::float,100::float,0::float,0::float,3034),'8BUI'::text, 0, null));


CREATE TABLE raster_500m (
	ras raster
);
INSERT INTO raster_500m VALUES (ST_AddBand(ST_MakeEmptyRaster(2000,2000,3584734::float,2997812::float,500::float,500::float,0::float,0::float,3034),'8BUI'::text, 0, null));


CREATE TABLE raster_1000m (
	ras raster
);
INSERT INTO raster_1000m VALUES (ST_AddBand(ST_MakeEmptyRaster(1000,1000,3584734::float,2997812::float,1000::float,1000::float,0::float,0::float,3034),'8BUI'::text, 0, null));

CREATE TABLE dim_cell (
	cell_id BIGSERIAL PRIMARY KEY,
	row_50m BIGINT,
	column_50m BIGINT,
	row_100m BIGINT,
	column_100m BIGINT,
	row_500m BIGINT,
	column_500m BIGINT,
	row_1000m BIGINT,
	column_1000m BIGINT,
	boundary_50m GEOMETRY,
	boundary_100m GEOMETRY,
	boundary_500m GEOMETRY,
	boundary_1000m GEOMETRY
)


EXPLAIN SELECT x, y,
	ceil(x/2.0), ceil(y/2.0),
	ceil(x/10.0), ceil(y/10.0),
	ceil(x/20.0), ceil(y/20.0),
 	ST_PixelAsPolygon(raster_50m.ras, x, y) --,
--    	ST_PixelAsPolygon(raster_100m.ras, x/2, y/2),
--    	ST_PixelAsPolygon(raster_500m.ras, x/10, y/10),
--    	ST_PixelAsPolygon(raster_1000m.ras, x/20, y/20)
	FROM generate_series(1, 10) x, generate_series(1, 10) y, 
		raster_50m, raster_100m, raster_500m, raster_1000m
		
SELECT x, y,
	ceil(x/2.0), ceil(y/2.0),
	ceil(x/10.0), ceil(y/10.0),
	ceil(x/20.0), ceil(y/20.0),
 	ST_PixelAsPolygon(raster_50m.ras, x, y) --,
--    	ST_PixelAsPolygon(raster_100m.ras, x/2, y/2),
--    	ST_PixelAsPolygon(raster_500m.ras, x/10, y/10),
--    	ST_PixelAsPolygon(raster_1000m.ras, x/20, y/20)
	FROM generate_series(1, 10) x, generate_series(1, 10) y, 
		raster_50m, raster_100m, raster_500m, raster_1000m
		
VACUUM ANALYZE raster_50m, raster_100m, raster_500m, raster_1000m

SELECT (ST_PixelAsPolygons(ras)).* from raster_1000m limit 100


SELECT * from
(SELECT x x50, y y50,
	ceil(x/2.0) x100, ceil(y/2.0) y100,
	ceil(x/10.0) x500, ceil(y/10.0) y500,
	ceil(x/20.0) x1000, ceil(y/20.0) y1000
FROM generate_series(1, 100) x, generate_series(1, 100) y) fooo join (SELECT (ST_PixelAsPolygons(ras)).* from raster_50m where (ST_PixelAsPolygons(ras)).x < 1000) foo on x = x50 and y = y50 -- join (SELECT (ST_PixelAsPolygons(ras)).* from raster_1000m) foooo on x = x50 and y = y50

SELECT (ST_PixelAsPolygons(ras)).* from raster_1000m
LIMIT 100
