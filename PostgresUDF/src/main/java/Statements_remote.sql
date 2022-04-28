SET pljava.libjvm_location TO '/home/student.aau.dk/knorho20/jdk-16/lib/server/libjvm.so';
ALTER DATABASE master SET pljava.libjvm_location FROM CURRENT;

CREATE EXTENSION pljava;


SELECT sqlj.install_jar(
               'file:/home/student.aau.dk/knorho20/PostgresUDF.jar', 'DecompressUDF', true
           );

select sqlj.set_classpath(
               'public', 'DecompressUDF'
           );
select sqlj.get_classpath('public');


DROP TYPE sqlDataPoint;
CREATE TYPE sqlDataPoint AS(timeSeriesId integer, timestamp BigInt, value float);


DROP FUNCTION decompressSegment(segment);
CREATE FUNCTION decompressSegment(segment)
    RETURNS Setof sqlDataPoint
AS 'SegmentDecompressor.decompressSegment'
    IMMUTABLE LANGUAGE java;

select * from timeseries;


561

select * from (
                  select (decompressSegment(s)).*
                  from segment s
                  where s.time_series_id = 561
                    and s.start_time <= 1304136472000
                    and (s.start_time + s.end_time) >= 1304136472000
              ) t where t.timestamp = 1304136472000
;
select count(*) from datapointview;