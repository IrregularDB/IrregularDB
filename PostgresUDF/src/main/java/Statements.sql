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