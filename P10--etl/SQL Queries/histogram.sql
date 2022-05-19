CREATE OR REPLACE FUNCTION hist_sfunc (state INTEGER[], val DOUBLE PRECISION,
       MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER) RETURNS INTEGER[] AS $$
DECLARE
  bucket INTEGER;
  i INTEGER;
BEGIN
  -- Do nothing if val is NULL
  IF val IS NULL THEN
     RETURN state;
  END IF;

  -- This will put values in buckets with a 0 bucket for <MIN and a (nbuckets+1) bucket for >=MAX
  bucket := width_bucket(val, MIN, MAX, nbuckets);
 
  -- Init the array with the correct number of 0's so the caller doesn't see NULLs
  IF state[0] IS NULL THEN
    state := array_fill(0,ARRAY[nbuckets+2],ARRAY[0]);
  END IF;

  state[bucket] := state[bucket] + 1;
 
  RETURN state;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Tell Postgres how to use the new function
DROP AGGREGATE IF EXISTS histogram (DOUBLE PRECISION, DOUBLE PRECISION, DOUBLE PRECISION, INTEGER);
CREATE AGGREGATE histogram (val DOUBLE PRECISION, min DOUBLE PRECISION, max DOUBLE PRECISION, nbuckets INTEGER) (
       SFUNC = hist_sfunc,
       STYPE = INTEGER[],
       PARALLEL = SAFE -- Remove line for compatibility with  Postgresql < 9.6
);

CREATE OR REPLACE FUNCTION histogram_ranges(MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER)
       RETURNS numrange[] AS
$$
DECLARE
	res numrange[];
BEGIN
	res := array_agg(numrange(l,u,'[)')) FROM
	(SELECT generate_series(MIN::numeric,(MAX-(MAX-MIN)/nbuckets)::numeric,((MAX-MIN)/nbuckets)::numeric) AS l,
       		generate_series((MIN+(MAX-MIN)/nbuckets)::numeric,MAX::numeric,((MAX-MIN)/nbuckets)::numeric) AS u) t;

	res[0] := numrange(NULL,MIN::numeric,'[)');
	res[nbuckets+1] := numrange(MAX::numeric,NULL,'[)');

	RETURN res;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

CREATE OR REPLACE FUNCTION histogram_breaks(MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER)
       RETURNS DOUBLE PRECISION[] AS
$$
SELECT array(SELECT generate_series(MIN::numeric,MAX::numeric,((MAX-MIN)/nbuckets)::numeric)::DOUBLE PRECISION)
;
$$ LANGUAGE sql IMMUTABLE;

CREATE OR REPLACE FUNCTION histogram_mids(MIN DOUBLE PRECISION, MAX DOUBLE PRECISION, nbuckets INTEGER)
       RETURNS DOUBLE PRECISION[] AS
$$
SELECT array(SELECT generate_series((MIN + 0.5*((MAX-MIN)/nbuckets))::numeric,
       		    	            MAX::numeric,
				    ((MAX-MIN)/nbuckets)::numeric)::DOUBLE PRECISION);
$$ LANGUAGE sql IMMUTABLE;

WITH a AS (
SELECT generate_series(1,5,0.5) AS i
)
SELECT array_agg(i) AS values,
--        histogram_mids(-0.1,25,250) AS mids,
       histogram_breaks(-0.1,25,251) AS breaks,
       histogram_ranges(-0.1,25,251) AS ranges,
       histogram(i,-0.1,25,250) AS counts,
       (histogram_ranges(-0.1,25,251))[1:25] AS ranges_in_limits,
       (histogram(i,0,3,3))[1:3] AS counts_in_limits
FROM a;