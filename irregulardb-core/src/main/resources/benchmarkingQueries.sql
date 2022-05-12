-- helper functions
create or replace function datapoints_in_buckets(theTimeSeriesId INTEGER, theStartTime bigint, theEndTime bigint,
                                                 theBucketSize integer)
    returns table(timeSeriesId integer,epochTime bigint,value real,bucketNumber integer)
as
$$
begin
    return query select res.id, res.epochtime, cast(res.value as real), res.bucketId
                 from (
                          select *, CAST((timestampRangeQuery.epochTime - theStartTime) / theBucketSize as INTEGER) as bucketId
                          from timestampRangeQuery(theTimeSeriesId, theStartTime, theEndtime, 0)
                      ) res;
end;
$$ language plpgsql;
select * from datapoints_in_buckets(55, 1303163035000, 1303170798000, 5000);






--No1 simple aggremate MAX for 1 time series every 5 minutes for 12 hours
--given start and end time, tid and interval time
--range query can give all the data points
drop function no1_1_12_max(theTimeSeriesId INTEGER, theStartTime bigint, theEndtime bigint, theIntervalSize INTEGER)
create or replace function no1_1_12_max(theTimeSeriesId INTEGER, theStartTime bigint, theEndtime bigint, theIntervalSize INTEGER)
    returns table(startTime bigint, endTime bigint,maxValue real) as $$
BEGIN
    return query select ((bucketNumber * theIntervalSize) + theStartTime) as startTime,
                        (bucketNumber * theIntervalSize + theStartTime + theIntervalSize) as endTime,
                        cast(max(value) as real) as maxValue
                 from datapoints_in_buckets(theTimeSeriesId, theStartTime, theEndtime, theIntervalSize)
                 group by bucketNumber
    ;
end;
$$ LANGUAGE plpgsql;


--MOSTLY COPY FROM NO1
--No2 simple aggregate AVG for 1 time series every 5 minutes for 12 hours
--given start and end time, tid and interval time
drop function no2_1_12_avg(theTimeSeriesId INTEGER, theStartTime bigint, theEndtime bigint, theIntervalSize INTEGER)
create or replace function no2_1_12_avg(theTimeSeriesId INTEGER, theStartTime bigint, theEndtime bigint, theIntervalSize INTEGER)
    returns table(startTime bigint, endTime bigint,maxValue real) as $$
BEGIN
    return query select ((bucketNumber * theIntervalSize) + theStartTime) as startTime,
                        (bucketNumber * theIntervalSize + theStartTime + theIntervalSize) as endTime,
                        cast(avg(value) as real) as avgValue
                 from datapoints_in_buckets(theTimeSeriesId, theStartTime, theEndtime, theIntervalSize)
                 group by bucketNumber
    ;
end;
$$ LANGUAGE plpgsql;


--NO3 Simple aggregate for 5 timeseries every 5 minutes for 12 hours
--NOT SURE If THiS Is OK
select ((bucketNumber * 5000) + 1303163035000)      as startTime,
       (bucketNumber * 5000 + 1303163035000 + 5000) as endTime,
       max(value)
from (
         select id,
                epochTime,
                value,
                CAST((epochTime - 1303163035000) / 5000 as INTEGER) as bucketNumber
         from (
                  select (timestampRangeQuery(id, 1303163035000, 1303170798000, 0)).*
                  from timeseries
                  where id in (55, 54, 56, 57, 58)
              ) res
     ) dpWithBucketNumber group by bucketNumber;


--NO4 Last datapoint for every timeseries
select latestDp.* from (
                           select timeseriesid, max(timestamp) as maxTimestamp from (select (decompresssegment(s)).* from segment s
                                                                                                                              join (select time_series_id, max(start_time) as start_time from segment group by time_series_id) maxStartTime
                                                                                                                                   on s.time_series_id = maxStartTime.time_series_id and s.start_time = maxStartTime.start_time
                                                                                    ) dataPoints group by timeseriesid) latestDpTime

                           join (select (decompresssegment(s)).* from segment s
                                                                          join (select time_series_id, max(start_time) as start_time from segment group by time_series_id) maxStartTime
                                                                               on s.time_series_id = maxStartTime.time_series_id and s.start_time = maxStartTime.start_time
) latestDp
                                on latestDpTime.timeseriesid = latestDp.timeseriesid and latestDpTime.maxTimestamp = latestDp.timestamp
;



--NO5 HighValue - simply use ValueRange query
select * from valueRangeQuery(28, 62, 63, 0.1, TRUE);
--NO6 value point - simply use ValuePoint query
select * from valuePointQuery(28, 64, 0.0, TRUE);
--NO7 timestamp point - simply use already created UDF
select * from timestampPointQuery(28, 1303132953000, 4000);
--NO8
select (decompresssegment(segment)).* from segment;


