drop FUNCTION IF EXISTS valueRangeQuery(timeSeriesId INTEGER, theMinValue real, theMaxValue real, errorBound real, useSegmentSummary boolean);
CREATE OR REPLACE FUNCTION valueRangeQuery(timeSeriesId INTEGER, theMinValue real, theMaxValue real, errorBound real, useSegmentSummary boolean)
    RETURNS TABLE(id INTEGER, timestamp BIGINT, value float) AS $$
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
                              where time_series_id = timeSeriesId and segment.minvalue >= lowerBound and upperBound <= segment.maxvalue
                              order by start_time
                          ) dp where lowerBound <= dp.value and dp.value <= upperBound;
    ELSE
        return query
            select * from (
                              select (decompresssegment(segment)).* from segment where time_series_id = timeSeriesId
                              order by start_time
                          ) dp where  lowerBound <= dp.value and dp.value <= upperBound;
    END IF;
END;
$$ LANGUAGE plpgsql;



drop FUNCTION IF EXISTS valuePointQuery(timeSeriesId INTEGER, theValue real, errorBound real, useSegmentSummary boolean);
CREATE OR REPLACE FUNCTION valuePointQuery(timeSeriesId INTEGER, theValue real, errorBound real, useSegmentSummary boolean)
    RETURNS TABLE(id INTEGER, timestamp BIGINT, value float) AS $$
    BEGIN
        return query
            select * from valueRangeQuery(timeSeriesId, theValue, theValue, errorBound, useSegmentSummary);

    END;
$$ LANGUAGE plpgsql;



drop FUNCTION IF EXISTS timestampRangeQuery(timeSeriesId INTEGER, theLowerBound BIGINT, theUpperBound BIGINT, threshold INTEGER);
CREATE OR REPLACE FUNCTION timestampRangeQuery(timeSeriesId INTEGER, theLowerBound BIGINT, theUpperBound BIGINT, threshold INTEGER)
    RETURNS TABLE(id INTEGER, timestamp BIGINT, value float) AS $$
DECLARE
    lowerBound constant BIGINT := theLowerBound - threshold;
    upperBound constant BIGINT := theUpperBound + threshold;
BEGIN
    return query
        select * from (
                          select (decompresssegment(segment)).* from segment
                          where time_series_id = timeSeriesId and segment.start_time <= lowerBound and upperBound <= (segment.start_time + segment.end_time)
                          order by start_time
                      ) dp where lowerBound <= dp.timestamp and dp.timestamp <= upperBound;
END;
$$ LANGUAGE plpgsql;



drop FUNCTION IF EXISTS timestampPointQuery(timeSeriesId INTEGER, theTimestamp BIGINT, threshold INTEGER);
CREATE OR REPLACE FUNCTION timestampPointQuery(timeSeriesId INTEGER, theTimestamp BIGINT, threshold INTEGER)
    RETURNS TABLE(id INTEGER, timestamp BIGINT, value float) AS $$
BEGIN
    return query
        select * from timestampRangeQuery(timeSeriesId, theTimestamp, theTimestamp, threshold);
END;
$$ LANGUAGE plpgsql;



select * from valuePointQuery(28, 64, 0.0, TRUE);
select * from valueRangeQuery(28, 62, 63, 0.1, TRUE);
select * from timestampPointQuery(28, 1303132953000, 4000);
select * from timestampRangeQuery(28, 1303132953000,1303132959000,4000);
