-- RANGE AND POINT QUERIES
drop FUNCTION IF EXISTS valueRangeQuery(timeSeriesId INTEGER, theMinValue real, theMaxValue real, errorBound real, useSegmentSummary boolean);
CREATE OR REPLACE FUNCTION valueRangeQuery(timeSeriesId INTEGER, theMinValue real, theMaxValue real, errorBound real, useSegmentSummary boolean)
    RETURNS TABLE(id INTEGER, epochTime BIGINT, value float) AS $$
DECLARE
    allowableErrorMinValue constant real := ABS(theMinValue * errorBound);
    allowableErrorMaxValue constant real := ABS(theMaxValue * errorBound);
    lowerBound constant real := theMinValue - allowableErrorMinValue;
    upperBound constant real := theMaxValue + allowableErrorMaxValue;
BEGIN
    IF useSegmentSummary THEN
        return query
            select * from (
                              select (decompresssegment(segment)).* from segment
                              where segment.time_series_id = timeSeriesId
                                and segment.minvalue <= upperBound AND lowerBound <= segment.maxvalue
                          ) dp where lowerBound <= dp.value and dp.value <= upperBound;
    ELSE
        return query
            select * from ( select (decompresssegment(segment)).* from segment where time_series_id = timeSeriesId) dp where  lowerBound <= dp.value and dp.value <= upperBound;
    END IF;
END;
$$ LANGUAGE plpgsql;



drop FUNCTION IF EXISTS valuePointQuery(timeSeriesId INTEGER, theValue real, errorBound real, useSegmentSummary boolean);
CREATE OR REPLACE FUNCTION valuePointQuery(timeSeriesId INTEGER, theValue real, errorBound real, useSegmentSummary boolean)
    RETURNS TABLE(id INTEGER, epochTime BIGINT, value float) AS $$
BEGIN
    return query
        select * from valueRangeQuery(timeSeriesId, theValue, theValue, errorBound, useSegmentSummary);

END;
$$ LANGUAGE plpgsql;



drop FUNCTION IF EXISTS timestampRangeQuery(timeSeriesId INTEGER, theLowerBound BIGINT, theUpperBound BIGINT, threshold INTEGER);
create function timestamprangequery(timeseriesid integer, thelowerbound bigint, theupperbound bigint, threshold integer)
    returns TABLE(id integer, epochtime bigint, value double precision)
    language plpgsql
as
$$
DECLARE
    lowerBound constant BIGINT := theLowerBound - threshold;
    upperBound constant BIGINT := theUpperBound + threshold;
BEGIN
    return query
        select * from (   select (decompresssegment(segment)).* from segment
                          where segment.time_series_id = timeSeriesId
                            and segment.start_time <= upperBound AND lowerBound <= (segment.start_time + segment.end_time)
                      ) dp where lowerBound <= dp.timestamp and dp.timestamp <= upperBound;
END;
$$;

drop FUNCTION IF EXISTS timestampPointQuery(timeSeriesId INTEGER, theTimestamp BIGINT, threshold INTEGER);
CREATE OR REPLACE FUNCTION timestampPointQuery(timeSeriesId INTEGER, theTimestamp BIGINT, threshold INTEGER)
    RETURNS TABLE(id INTEGER, epochTime BIGINT, value float) AS $$
BEGIN
    return query
        select * from timestampRangeQuery(timeSeriesId, theTimestamp, theTimestamp, threshold);
END;
$$ LANGUAGE plpgsql;

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


