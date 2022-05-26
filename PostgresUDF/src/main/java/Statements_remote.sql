SET pljava.libjvm_location TO '/home/student.aau.dk/knorho20/jdk-16/lib/server/libjvm.so';

ALTER DATABASE irregulardb_irregular_no_summary SET pljava.libjvm_location FROM CURRENT;
ALTER DATABASE irregulardb_irregular_summary SET pljava.libjvm_location FROM CURRENT;
ALTER DATABASE irregulardb_regular_no_summary SET pljava.libjvm_location FROM CURRENT;
ALTER DATABASE irregulardb_regular_summary SET pljava.libjvm_location FROM CURRENT;
ALTER DATABASE irregulardb_tsbs_error_1_no_sum SET pljava.libjvm_location FROM CURRENT;
ALTER DATABASE irregulardb_tsbs_error_1_sum SET pljava.libjvm_location FROM CURRENT;
ALTER DATABASE irregulardb_tsbs_no_sum SET pljava.libjvm_location FROM CURRENT;
ALTER DATABASE irregulardb_tsbs_sum SET pljava.libjvm_location FROM CURRENT;


CREATE EXTENSION pljava;

SELECT sqlj.remove_jar('DecompressUDF', true);

SELECT sqlj.install_jar(
               'file:/home/student.aau.dk/knorho20/PostgresUDF16.jar', 'DecompressUDF', true
           );

select sqlj.set_classpath(
               'public', 'DecompressUDF'
           );
select sqlj.get_classpath('public');


part 1 done, part 2 done, part 3 ongoing
select min(id) from timeseries; 197 -> 225 -> 255 -> 285 -> 312
select max(id) from timeseries; 312
drop materialized view datapointsview;
create materialized view datapointsview as
select tag, timestamp, value
from (
         select (decompressSegment(segment)).*
         from segment --where time_series_id >= 285 and time_series_id < 999
     ) dp join timeseries t
               on dp.timeseriesid = t.id
order by tag, timestamp
;




select * from timeseries;
--find segment with timestamp
select * from segment
select (decompressSegment(segment)).* from segment where start_time <= 1304728429000 and (end_time + segment.start_time) >= 1304728429000 and time_series_id =427;
select start_time + end_time, * from segment where start_time <= 1304728429000 and (end_time + segment.start_time) >= 1304728429000 and time_series_id =427;
select distinct value_timestamp_model_type from segment;

select * from (
                  select (decompressSegment(s)).*
                  from segment s
                  where s.time_series_id = 561
                    and s.start_time <= 1304136472000
                    and (s.start_time + s.end_time) >= 1304136472000
              ) t where t.timestamp = 1304136472000
;
select count(*) from datapointview;

select minvalue, maxvalue, amtdatapoints, start_time + end_time, * from segment where start_time = 1303132933000 and time_series_id = 39;
select (decompresssegment(segment)).* from segment where start_time = 1303132933000 and time_series_id = 39;