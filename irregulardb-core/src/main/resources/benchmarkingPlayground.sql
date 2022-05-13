vacuum full analyse ;
-- 5 minutes in miliseconds = 300000
select * from timeseries;
select (decompresssegment(segment)).* from segment where time_series_id = 118;

---------------------- NO 1 --------------------
select * from timeseries where
        tag like '/house_4_0.823/channel_7_sorted.csv'


--No1 simple aggremate MAX for 1 time series every 5 minutes for 12 hours
--given start and end time, tid and interval time
--range query can give all the data points
drop function no1_1_12_avg(theTimeSeriesId INTEGER, theStartTime bigint, theEndtime bigint, theIntervalSize INTEGER)
create or replace function no1_1_12_avg(theTimeSeriesId INTEGER, theStartTime bigint, theEndtime bigint, theIntervalSize INTEGER)
    returns table(startTime bigint, endTime bigint,maxValue real) as $$
BEGIN
return query select ((bucketNumber * theIntervalSize) + theStartTime) as startTime,
                        (bucketNumber * theIntervalSize + theStartTime + theIntervalSize) as endTime,
                        cast(avg(value) as real) as maxValue
                 from datapoints_in_buckets(theTimeSeriesId, theStartTime, theEndtime, theIntervalSize)
                 group by bucketNumber
;
end;
$$ LANGUAGE plpgsql;


select * from no1_1_12_avg(9130, 1303002992000, 1303046192000, 300000)
;




---------------------- NO 2 --------------------
--NO3 Simple aggregate for 5 timeseries every 5 minutes for 12 hours
--NOT SURE If THiS Is OK
--bucketSize = 300000,
-- NO2
select * from timeseries where
        tag like '/house_4_0.823/channel_3_sorted.csv'
                            OR tag like '/house_4_0.823/channel_4_sorted.csv'
                            OR tag like '/house_4_0.823/channel_5_sorted.csv'
                            OR tag like '/house_4_0.823/channel_6_sorted.csv'
                            OR tag like '/house_4_0.823/channel_7_sorted.csv'
;

select ((bucketNumber * 300000) + 1303002992000)      as startTime,
       (bucketNumber * 300000 + 1303002992000 + 300000) as endTime,
       max(value)
from (
         select id,
                epochTime,
                value,
                CAST((epochTime - 1303002992000) / 300000 as INTEGER) as bucketNumber
         from (
                  select (timestampRangeQuery(id, 1303002992000, 1303046192000, 0)).*
                  from timeseries
                  where id in (6688, 9130, 14186, 15804, 21137)
              ) res
     ) dpWithBucketNumber group by bucketNumber;




---------------------- NO 3 --------------------
select res.timeseriesid, res.timestamp,res.value  from (
                                                           select s.start_time + s.end_time as latestTimestamp, (decompresssegment(s)).*
                                                           from (
                                                               select time_series_id, max(start_time) as start_time from segment group by time_series_id
                                                               ) lastSegmentTime
                                                               join segment s on s.start_time = lastSegmentTime.start_time and
                                                               s.time_series_id = lastSegmentTime.time_series_id
                                                       ) res where res.latestTimestamp = res.timestamp
;

---------------------- NO 4 --------------------
select * from timeseries where
        tag like '/house_1-8.879/channel_3_sorted.csv'
;
--NO4 HighValue - simply use ValueRange query
select * from valueRangeQuery(23533, 13318.5, 2000000000, 0, TRUE);


---------------------- NO 5 --------------------
--NO5
select * from timeseries where
        tag like '/house_5-4.322/channel_12_sorted.csv'
;
--NO5 timestamp point - simply use already created UDF
select * from timestampPointQuery(14903, 1303134414000, 500);



---------------------- NO 6 --------------------
select * from timeseries where
        tag like '/house_6-5.716/channel_16_sorted.csv'
;

--NO6 value point - simply use ValuePoint query
select * from valuePointQuery(24814, 318.5475612997497, 0.1, TRUE);






---------------------- NO 7 --------------------
--NO7
select * from timeseries where
        tag like '/house_2-6.416/channel_4_sorted.csv'
;
--NO7
select (decompresssegment(segment)).* from segment where time_series_id = 9317;




