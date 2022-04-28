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