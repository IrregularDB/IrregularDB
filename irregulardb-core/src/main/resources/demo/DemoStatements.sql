select * from segment;
select * from timeseries;

-- Gets amount of segments of each type of model
SELECT count(*), t.valuemodel, t.timestampmodel FROM segment s
    JOIN timestampvaluemodeltypes t ON s.value_timestamp_model_type = t.timestampvaluemodelshort
    GROUP BY t.valuemodel, t.timestampmodel;

-- Get all data points for key 2
SELECT (decompressSegment(segment)).* FROM segment
    JOIN timeseries t ON segment.time_series_id = t.id
    WHERE t.tag = 'key2';

select t.id from timeseries t where tag = 'key2';

select * from valuePointQuery(208811, 2, 0.0001, TRUE);

select * from timestampRangeQuery(208811, 3000, 5000, 0);

truncate timeseries cascade;


-- UDFS:
drop FUNCTION IF EXISTS valueRangeQuery(timeSeriesId INTEGER, theMinValue real, theMaxValue real, errorBound real, useSegmentSummary boolean);
CREATE OR REPLACE FUNCTION valueRangeQuery(timeSeriesId INTEGER, theMinValue real, theMaxValue real, errorBound real, useSegmentSummary boolean)
    RETURNS TABLE(id INTEGER, epochTime BIGINT, value real)
AS $$
DECLARE
    allowableErrorMinValue constant real := ABS(theMinValue * errorBound);
    allowableErrorMaxValue constant real := ABS(theMaxValue * errorBound);
    lowerBound real := theMinValue - allowableErrorMinValue;
    upperBound real := theMaxValue + allowableErrorMaxValue;
BEGIN
    IF useSegmentSummary THEN
        return query
            select * from (
                              select (decompresssegment(segment)).* from segment
                              where segment.time_series_id = timeSeriesId
                                and segment.minvalue <= upperBound
                                AND segment.maxvalue >= lowerBound
                          ) dp where lowerBound <= dp.value and dp.value <= upperBound;
    ELSE
        return query
            select * from ( select (decompresssegment(segment)).* from segment where time_series_id = timeSeriesId) dp where  lowerBound <= dp.value and dp.value <= upperBound;
    END IF;
END;
$$ LANGUAGE plpgsql;


drop FUNCTION IF EXISTS valuePointQuery(timeSeriesId INTEGER, theValue real, errorBound real, useSegmentSummary boolean);
CREATE OR REPLACE FUNCTION valuePointQuery(timeSeriesId INTEGER, theValue real, errorBound real, useSegmentSummary boolean)
    RETURNS TABLE(id INTEGER, epochTime BIGINT, value real) AS $$
BEGIN
    return query
        select * from valueRangeQuery(timeSeriesId, theValue, theValue, errorBound, useSegmentSummary);

END;
$$ LANGUAGE plpgsql;



drop FUNCTION IF EXISTS timestampRangeQuery(timeSeriesId INTEGER, theLowerBound BIGINT, theUpperBound BIGINT, threshold INTEGER);
CREATE OR REPLACE FUNCTION timestamprangequery(timeseriesid integer, thelowerbound bigint, theupperbound bigint, threshold integer)
    returns TABLE(id integer, epochtime bigint, value real)
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
    RETURNS TABLE(id INTEGER, epochTime BIGINT, value real) AS $$
BEGIN
    return query
        select * from timestampRangeQuery(timeSeriesId, theTimestamp, theTimestamp, threshold);
END;
$$ LANGUAGE plpgsql;