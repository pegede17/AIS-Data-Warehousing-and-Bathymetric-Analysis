-- 1000m 700 celler 7 dage 
SELECT 
   json_build_object(
     'type', 
     'FeatureCollection', 
     'features', 
     json_agg(
       ST_AsGeoJSON(result.*):: json
     )
   ) 
 FROM 
  (
    SELECT 
      ST_Transform(boundary, 4326), 
      max_draught, 
      min_draught, 
      trajectory_count, 
      cell_data.cell_id, 
      avg_draught 
    FROM 
      dim_cell_3034_1000m d 
      inner join (
        SELECT 
          MAX(max_draught) max_draught, 
          MIN(min_draught) min_draught, 
          cell_id, 
          SUM(trajectory_count) trajectory_count, 
          SUM(avg_draught * trajectory_count)/ CASE WHEN SUM(
            CASE WHEN avg_draught IS NOT NULL 
                 THEN trajectory_count ELSE 0 END
          ) > 0 THEN SUM(
            CASE WHEN avg_draught IS NOT NULL 
                 THEN trajectory_count ELSE 0 END
          ) ELSE 1 END avg_draught 
        from 
          fact_cell_3034_1000m
        WHERE 
          cell_id in (
            SELECT 
              cell_id 
            FROM 
              dim_cell_3034_1000m 
            WHERE 
              ST_Intersects(
                boundary, 
                ST_Transform(
                  ST_MakeEnvelope (
                    9.335632324218752, 
					55.490812118753104, 
					14.372351074218752, 
					55.41493571902954,
                    4326
                  ), 
                  3034
                )
              )
          ) 
          AND date_id BETWEEN 20210501 AND 20210507 
          AND ship_type_id IN (
            SELECT 
              ship_type_id 
            from 
              dim_ship_type 
            WHERE 
              ship_type IN (
                'Unknown', 'Other', 'Passenger', 'Cargo', 
                'Pilot', 'Tanker', 'Sailing', 'Fishing', 
                'Dredging', 'SAR', 'Undefined', 'HSC', 
                'Tug', 'Port tender', 'Reserved', 
                'Not party to conflict', 'Military', 
                'Law enforcement', 'Pleasure', 'Diving', 
                'Towing long/wide', 'Anti-pollution', 
                'Towing', 'Medical', 'Spare 2', 'Spare 1', 
                'WIG'
              )
          ) 
          AND type_of_mobile_id IN (
            SELECT 
              type_of_mobile_id 
            from 
              dim_type_of_mobile 
            WHERE 
              mobile_type IN (
                'Unknown', 'Class A', 'Class B', 'Base Station', 
                'AtoN', 'SAR Airborne', 'Search and Rescue Transponder'
              )
          ) 
--           AND is_draught_trusted 
        GROUP BY 
          cell_id
      ) cell_data on cell_data.cell_id = d.cell_id
   ) as result(
     geom, maxDraught, minDraught, trajectory_count, 
     cellId, avgDraught
   );