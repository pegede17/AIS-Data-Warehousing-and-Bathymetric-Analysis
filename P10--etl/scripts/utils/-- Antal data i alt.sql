-- Antal data i alt 
-- fact_ais
SELECT count(1)
FROM fact_ais
WHERE ts_date_id BETWEEN 20210501 AND 20210930

Result 1860699414

-- fact_ais_clean
SELECT count(1)
FROM fact_ais_clean
WHERE ts_date_id BETWEEN 20210501 AND 20210930

Result 1641590137

-- fact_trajectory_sailing
SELECT count(1)
FROM fact_trajectory_sailing
WHERE date_start_id BETWEEN 20210501 AND 20210930

Result 2514635

-- fact_trajectory_stopped
SELECT count(1)
FROM fact_trajectory_stopped
WHERE date_start_id BETWEEN 20210501 AND 20210930

Result 8494357

-- fact_cell_50m
SELECT count(1)
FROM fact_cell_3034_50m
WHERE date_id BETWEEN 20210501 AND 20210930

Result 814984634

-- fact_cell_1000m
SELECT count(1)
FROM fact_cell_3034_1000m
WHERE date_id BETWEEN 20210501 AND 20210930

Result 22896139

-- Skibstype fordeling sailing
SELECT ship_type, count(*)
FROM fact_trajectory_sailing f
JOIN dim_ship_type d
    ON f.ship_type_id = d.ship_type_id 
GROUP BY ship_type

"Anti-pollution"	1507
"Cargo"	297686
"Diving"	4010
"Dredging"	71765
"Fishing"	255307
"HSC"	64864
"Law enforcement"	15815
"Medical"	8
"Military"	12524
"Not party to conflict"	3577
"Other"	155251
"Passenger"	471741
"Pilot"	39058
"Pleasure"	338567
"Port tender"	8969
"Reserved"	6279
"SAR"	52592
"Sailing"	469023
"Spare 1"	3760
"Spare 2"	2283
"Tanker"	94963
"Towing"	6654
"Towing long/wide"	980
"Tug"	90298
"Undefined"	46833
"WIG"	321

-- Skibstype fordeling stopped
SELECT ship_type, count(*)
FROM fact_trajectory_stopped f
JOIN dim_ship_type d
    ON f.ship_type_id = d.ship_type_id 
GROUP BY ship_type

"Anti-pollution"	26634
"Cargo"	482642
"Diving"	22744
"Dredging"	181919
"Fishing"	857464
"HSC"	138446
"Law enforcement"	59774
"Medical"	69
"Military"	87156
"Not party to conflict"	4725
"Other"	364895
"Passenger"	729181
"Pilot"	133557
"Pleasure"	2164630
"Port tender"	47182
"Reserved"	28924
"SAR"	271483
"Sailing"	2243605
"Spare 1"	12250
"Spare 2"	3172
"Tanker"	205586
"Towing"	31012
"Towing long/wide"	3254
"Tug"	282250
"Undefined"	110633
"WIG"	1170

-- Type of mobile fordeling sailing
SELECT mobile_type, count(*)
FROM fact_trajectory_sailing f
JOIN dim_type_of_mobile d
    ON f.type_of_mobile_id = d.type_of_mobile_id 
GROUP BY mobile_type

"Class A"	1574626
"Class B"	940009

-- Type of mobile fordeling stopped
SELECT mobile_type, count(*)
FROM fact_trajectory_stopped f
JOIN dim_type_of_mobile d
    ON f.type_of_mobile_id = d.type_of_mobile_id 
GROUP BY mobile_type

"Class A"	3559711
"Class B"	4934646

-- Punkter brugt til sailing
SELECT sum(total_points)
FROM fact_trajectory_sailing
WHERE date_start_id BETWEEN 20210501 AND 20210930

Result 901277742

-- Punkter brugt til stopped
SELECT sum(total_points)
FROM fact_trajectory_stopped
WHERE date_start_id BETWEEN 20210501 AND 20210930

Result 716566647

-- Sailing not null draught
SELECT count(1)
FROM fact_trajectory_sailing
WHERE date_start_id BETWEEN 20210501 AND 20210930
AND draught IS NOT NULL

Result 1374334

-- Stopped not null draught
SELECT count(1)
FROM fact_trajectory_stopped
WHERE date_start_id BETWEEN 20210501 AND 20210930
AND draught IS NOT NULL

Result 3103524

-- fact_ais not null draught
SELECT count(1)
FROM fact_ais
WHERE ts_date_id BETWEEN 20210501 AND 20210930
AND draught IS NOT NULL

Result 1289473440

-- fact_ais_clean not null draught
SELECT count(1)
FROM fact_ais_clean
WHERE ts_date_id BETWEEN 20210501 AND 20210930
AND draught IS NOT NULL

Result 1232279148

-- Størrele på tabeller og indices
WITH RECURSIVE tables AS (
  SELECT
    c.oid AS parent,
    c.oid AS relid,
    1     AS level
  FROM pg_catalog.pg_class c
  LEFT JOIN pg_catalog.pg_inherits AS i ON c.oid = i.inhrelid
    -- p = partitioned table, r = normal table
  WHERE c.relkind IN ('p', 'r')
    -- not having a parent table -> we only get the partition heads
    AND i.inhrelid IS NULL
  UNION ALL
  SELECT
    p.parent         AS parent,
    c.oid            AS relid,
    p.level + 1      AS level
  FROM tables AS p
  LEFT JOIN pg_catalog.pg_inherits AS i ON p.relid = i.inhparent
  LEFT JOIN pg_catalog.pg_class AS c ON c.oid = i.inhrelid AND c.relispartition
  WHERE c.oid IS NOT NULL
)
SELECT
  parent ::REGCLASS                                  AS table_name,
  array_agg(relid :: REGCLASS)                       AS all_partitions,
  pg_size_pretty(sum(pg_total_relation_size(relid))) AS pretty_total_size,
  pg_size_pretty(sum(pg_indexes_size(relid))) 		   AS pretty_index_size,
  sum(pg_total_relation_size(relid))                 AS total_size
FROM tables
GROUP BY parent
ORDER BY sum(pg_total_relation_size(relid)) DESC

-- Unikke ship_ids i trajectories
SELECT count(*) from (SELECT DISTINCT(ship_id) from fact_trajectory_sailing UNION SELECT DISTINCT(ship_id) from fact_trajectory_stopped) foo

Result 25573

-- Unikke ship_ids i sailing trajectories 
SELECT count(distinct(ship_id)) from fact_trajectory_sailing

Result 24572

-- Unikke ship_ids i stopped trajectories
SELECT count(distinct(ship_id)) from fact_trajectory_stopped

Result 22064

-- Unikke ship_ids i rå data
SELECT count(distinct(ship_id)) from fact_ais

Result 88549

-- Unikke ship_ids i cleaned data
SELECT count(distinct(ship_id)) from fact_ais_clean

Result 26092

-- Unikke mmsi
SELECT count(distinct(mmsi)) from dim_ship

-- Antal skibe i dim_ship
SELECT count(1) from dim_ship

Result 46708

-- Antal skibe med IMO
SELECT count(imo) from dim_ship

Result 17394

-- Antal skibe der har trajectories med IMO
SELECT count(*) from (SELECT DISTINCT(ship_id) from fact_trajectory_sailing UNION SELECT DISTINCT(ship_id) from fact_trajectory_stopped) foo 
INNER JOIN dim_ship on foo.ship_id = dim_ship.ship_id
WHERE IMO IS NOT NULL

Result 8362

-- Antal unikke skibe med IMO
SELECT count(DISTINCT(imo)) from dim_ship
Result 11912

-- Antal trustede trajectories i fact_trajectory_sailing
SELECT count(1) from fact_trajectory_sailing where is_draught_trusted
Result 1274658

-- Antal trustede trajectories i fact_trajectory_stopped
SELECT count(1) from fact_trajectory_stopped where is_draught_trusted
3102180

SELECT count(1) from fact_ais
WHERE type_of_mobile_id = 3

Result 314016207

SELECT count(1) from fact_ais_clean
WHERE type_of_mobile_id = 3


-- fact ais null draught
1860699414 - 1289473440 = 571.225.974

-- fact ais clean null draught
1641590137 - 1232279148 = 409.310.989

-- fact ais clean null draught type B
SELECT count(1)
FROM fact_ais_clean
WHERE ts_date_id BETWEEN 20210501 AND 20210930
AND draught IS NULL
AND type_of_mobile_id = 3
184.659.922

-- fact ais type b null
213479234

-- fact_ais_clean type b null
184659922



with buckets as (SELECT 10 bottom, 15 top),
magicnumber as (select 500 x, count(trajectory_id) mysum, SUM(CASE WHEN draught[1] >= bottom AND draught[1] < top THEN 1 ELSE 0 END) total from
 fact_trajectory_sailing, buckets) ,
bins as (
    SELECT 
        floor(((width * length)) / x) * x as bin_floor,
        count(trajectory_id) as counter
    FROM magicnumber, buckets, public.fact_trajectory_sailing
        inner join dim_ship
        on fact_trajectory_sailing.ship_id = dim_ship.ship_id
        where draught[1] >= bottom AND draught[1] < top


    group by 1
    order by 1
)
SELECT 
    bin_floor,
--     bin_floor ||  ' - ' || (bin_floor + x) as bin_range,
    counter,
    (counter::double precision / mysum:: double precision) * 100 as procent,
    (counter::double precision / total:: double precision) * 100 as bucket_procent
from bins, magicnumber
order by 1;