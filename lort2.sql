COPY(
SELECT fact_id, eta_date_id, eta_time_id, foo.ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, type_of_mobile_id, navigational_status_id, cargo_type_id, type_of_position_fixing_device_id, ship_type_id, coordinate, draught, rot, sog, cog, heading, 1
	FROM (SELECT DISTINCT ON (eta_date_id, eta_time_id, ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, type_of_mobile_id, navigational_status_id, cargo_type_id, type_of_position_fixing_device_id, ship_type_id, coordinate, draught, rot, sog, cog, heading) * FROM fact_ais) foo
			INNER JOIN public.dim_ship on foo.ship_id = dim_ship.ship_id, public.danish_waters
    WHERE 
		audit_id = 1
        AND mmsi > 99999999
        AND mmsi < 1000000000 
        AND ST_Contains(geom ,coordinate::geometry)
) TO '/var/tmp/clean_ais_fact_27.csv' DELIMITER ',';

COPY fact_ais_clean_v2 from '/var/tmp/clean_ais_fact_audit_1.csv' DELIMITER ',';
		
(COPY (SELECT fact_id, eta_date_id, eta_time_id, fact_ais.ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, type_of_mobile_id, navigational_status_id, cargo_type_id, type_of_position_fixing_device_id, ship_type_id, coordinate, draught, rot, sog, cog, heading, 1
	FROM fact_ais INNER JOIN public.dim_ship on fact_ais.ship_id = dim_ship.ship_id, public.danish_waters
    WHERE 
		audit_id = 1
        AND mmsi > 99999999
        AND mmsi < 1000000000 
        AND fact_id IN (SELECT DISTINCT ON (eta_date_id, eta_time_id, ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, type_of_mobile_id, navigational_status_id, cargo_type_id, type_of_position_fixing_device_id, ship_type_id, coordinate, draught, rot, sog, cog, heading) fact_id FROM fact_ais WHERE audit_id = 1)
        AND ST_Contains(geom ,coordinate::geometry)) TO fact_ais_clean_v2)
		
SELECT * FROM(
SELECT fact_id, eta_date_id, eta_time_id, foo.ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, type_of_mobile_id, navigational_status_id, cargo_type_id, type_of_position_fixing_device_id, ship_type_id, coordinate, draught, rot, sog, cog, heading, 1
	FROM (SELECT DISTINCT ON (eta_date_id, eta_time_id, ship_id, ts_date_id, ts_time_id, data_source_type_id, destination_id, type_of_mobile_id, navigational_status_id, cargo_type_id, type_of_position_fixing_device_id, ship_type_id, coordinate, draught, rot, sog, cog, heading) * FROM (SELECT * FROM fact_ais WHERE audit_id = 1) fee) foo
			INNER JOIN public.dim_ship on foo.ship_id = dim_ship.ship_id, public.danish_waters
    WHERE 
		audit_id = 1
        AND mmsi > 99999999
        AND mmsi < 1000000000 
        AND ST_Contains(geom ,coordinate::geometry)) foo
LIMIT 10