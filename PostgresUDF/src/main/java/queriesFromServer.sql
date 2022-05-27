--no1
select 'no 1';
select * from no1_1_12_max(161,1303132933000,1451649600000,300000);


--no2
select 'no 2';
select ((bucketNumber * 300000) + 1303132933000)      as startTime,
       (bucketNumber * 300000 + 1303132933000 + 300000) as endTime,
       max(value)
from (
         select id,
                epochTime,
                value,
                CAST((epochTime - 1303132933000) / 300000 as BIGINT) as bucketNumber
         from (
                  select (timestampRangeQuery(id, 0, 1451649600000, 0)).*
                  from timeseries
                  where id in (161,162,163,164,165)
              ) res
     ) dpWithBucketNumber group by bucketNumber;

--no3
select 'no 3';
select res.timeseriesid, res.timestamp,res.value  from (
                                                           select s.start_time + s.end_time as latestTimestamp, (decompresssegment(s)).*
                                                           from (
                                                                    select time_series_id, max(start_time) as start_time from segment group by time_series_id
                                                                ) lastSegmentTime
                                                                    join segment s on s.start_time = lastSegmentTime.start_time and
                                                                                      s.time_series_id = lastSegmentTime.time_series_id
                                                       ) res where res.latestTimestamp = res.timestamp
;

--NO4 HighValue - simply use ValueRange query
select 'no 4';
select * from valueRangeQuery(161,90, 2000000000, 0, FALSE);

--no 5
select 'no 5';
select * from timestampPointQuery(161, 1454075479700, 500);

--no6
select 'no6';
select * from valuePointQuery(161, 40000, 0.1, FALSE);

--no7
select 'no 7';
select (decompresssegment(segment)).* from segment where time_series_id = 2236;
