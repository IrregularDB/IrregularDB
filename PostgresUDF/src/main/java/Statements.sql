--ONE TIME SETUP
SET pljava.libjvm_location TO '/usr/lib/jvm/java-17-openjdk-amd64/lib/server/libjvm.so';
ALTER DATABASE postgres SET pljava.libjvm_location FROM CURRENT;

CREATE EXTENSION pljava;

truncate table sqlj.classpath_entry;
truncate table sqlj.jar_descriptor;
truncate table sqlj.typemap_entry;
truncate table sqlj.jar_repository cascade ;
truncate table sqlj.jar_entry CASCADE ;


DROP FUNCTION decompressSegment;
DROP FUNCTION hello;
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
CREATE TYPE sqlDataPoint AS(timeSeriesId integer, timestamp BigInt, value float);


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
