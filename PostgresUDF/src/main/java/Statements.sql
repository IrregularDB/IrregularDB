--ONE TIME SETUP
SET pljava.libjvm_location TO '/usr/lib/jvm/java-17-openjdk-amd64/lib/server/libjvm.so';
ALTER DATABASE postgres SET pljava.libjvm_location FROM CURRENT;

CREATE EXTENSION pljava;

SELECT sqlj.remove_jar(
    'DecompressUDF', true);

SELECT sqlj.install_jar(
               'file:/home/simon/Development/IrregularDB/PostgresUDF/target/PostgresUDF-1.0-SNAPSHOT-jar-with-dependencies.jar', 'DecompressUDF', true
);

select sqlj.set_classpath(
    'public', 'DecompressUDF'
);
select sqlj.get_classpath('public');

DROP TYPE sqlDataPoint;
CREATE TYPE sqlDataPoint AS(timeSeriesId integer, timestamp BigInt, value real);


DROP FUNCTION decompressSegment(segment);
CREATE FUNCTION decompressSegment(segment)
    RETURNS Setof sqlDataPoint
    AS 'SegmentDecompressor.decompressSegment'
    IMMUTABLE LANGUAGE java;


--example query
select (decompressSegment(segment)).* from segment
    where time_series_id = 1
;

-- example of using query as a view
create view datapointview as
    select (decompressSegment(segment)).*
    from segment
    where time_series_id = 1
;

-- example of selecting from view
select * from datapointview dp
    join timeseries ts
        on dp.timeseriesid = ts.id
;

select count(*) from segment;

-- Gets amount of segments of each type of model
select count(*), t.valuemodel, t.timestampmodel from segment s
    join timestampvaluemodeltypes t on s.value_timestamp_model_type = t.timestampvaluemodelshort
    group by t.valuemodel, t.timestampmodel;

-- Get amount of each type of value models
select count(*), t.valuemodel from segment s
    join timestampvaluemodeltypes t on s.value_timestamp_model_type = t.timestampvaluemodelshort
    group by t.valuemodel;

select count(*), t.timestampmodel from segment s
    join timestampvaluemodeltypes t on s.value_timestamp_model_type = t.timestampvaluemodelshort
group by t.timestampmodel;

VACUUM FULL ANALYZE;
SELECT pg_database.datname as "database_name", pg_database_size(pg_database.datname) FROM pg_database;

SELECT pg_total_relation_size('timeseries');
SELECT pg_total_relation_size('segment');
select pg_column_size(segment.*);
SELECT pg_total_relation_size('timestampvaluemodeltypes');
SELECT pg_total_relation_size('segmentsummary');

SELECT pg_size_pretty( pg_total_relation_size('timeseries'));



-- EVALUATION QUERIES:
select m.timestampmodel, m.valuemodel, amt_of_model
from (
         select count(*) as amt_of_model, segment.value_timestamp_model_type as mtid
         from segment
         where segment.end_time < 399 * 100
         group by segment.value_timestamp_model_type) t
     join public.timestampvaluemodeltypes m on t.mtid = m.timestampvaluemodelshort;

-- Get total amount of segments below half of lenght
select count(*)
from segment
where segment.end_time < (200-1) * 100;

vacuum full analyse;

-- Get total amount of segments above length bound:
select count(*)
from segment
where segment.end_time > (400-1) * 100;

-- MODELARDB QUERIES:
-- GETS AMOUNT OF SEGMENTS OF EACH TYPE WITH LENGHT LESS THAN 400
select model_type.name, t.amt_of_model
from (
         select count(*) as amt_of_model, segment.mtid
         from segment
         where (segment.end_time - segment.start_time) < 399 * 100
         group by segment.mtid) t
    join model_type on t.mtid = model_type.mtid;