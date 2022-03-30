SET pljava.libjvm_location TO '/usr/lib/jvm/java-17-openjdk-amd64/lib/server/libjvm.so';

ALTER DATABASE postgres SET pljava.libjvm_location FROM CURRENT;

SELECT sqlj.remove_jar(
               'myjar', true);

SELECT sqlj.install_jar(
               'file:/home/simon/Development/IrregularDB/PostgresUDF/target/PostgresUDF-1.0-SNAPSHOT-jar-with-dependencies.jar', 'myjar', true);
);

SELECT sqlj.install_jar(
               'file:/home/simon/Development/IrregularDB/Compression/target/Compression-1.0-SNAPSHOT.jar', 'compressionJar', true);
);


select sqlj.set_classpath('public', 'myjar');

select sqlj.get_classpath('public');

select test(0);

select hellos('some inpiut');


CREATE TYPE sqlDataPoint AS(timeSeriesId integer, timestamp BigInt, value float);

CREATE FUNCTION useComplexTest()
    RETURNS sqlDataPoint
    AS 'UDFs.decompress'
    IMMUTABLE LANGUAGE java;

CREATE FUNCTION useSetOfComplexTest(timeSeriesId int, startTime bigint, endTime int, valueTimestampModelType smallint,
                                    valueModelBlob bytea, timestampModelBlob bytea)
    RETURNS Setof sqlDataPoint
    AS 'SetOfComplexTypeUDF.listComplexTest'
    IMMUTABLE LANGUAGE java;

DROP TYPE sqlDataPoint;

select (useComplexTest());

select hellos(colA), hellos(colA) from kenneth;

select * from decompress();

select c.* from (select useComplexTest() from kenneth) c;

select * from useComplexTest();

select useSetOfComplexTest(time_series_id, start_time, end_time, value_timestamp_model_type, value_model_blob, timestamp_model_blob) from segment where time_series_id = 1;

