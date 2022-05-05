DROP materialized view IF EXISTS datapointsview;
DROP FUNCTION IF EXISTS decompressSegment(Segment);
DROP TYPE IF EXISTS sqlDataPoint;

DROP TABLE IF EXISTS TimestampValueModelTypes;
DROP TABLE IF EXISTS SegmentSummary;
DROP TABLE IF EXISTS Segment;
DROP TABLE IF EXISTS TimeSeries;
DROP SEQUENCE IF EXISTS TimeSeriesIdSequence;

create sequence TimeSeriesIdSequence;

CREATE TABLE TimeSeries(
    id int not null DEFAULT nextval('TimeSeriesIdSequence'),
    tag VARCHAR(255) not null,
    CONSTRAINT pk_timeSeries_id PRIMARY KEY(id),
    constraint unique_timeSeries_tag unique(tag)
);

CREATE TABLE Segment(
    time_series_id int not null,
    start_time bigint not null,
    end_time int not null,
    value_timestamp_model_type int2 not null,
    value_model_blob bytea not null,
    timestamp_model_blob bytea not null,
    minValue real,
    maxValue real,
    amtDataPoints int,
    CONSTRAINT fk_time_series
                    FOREIGN KEY(time_series_id)
                    REFERENCES TimeSeries(id)
                    ON DELETE CASCADE,
    CONSTRAINT pk_segment_timeId_startTime primary key(time_series_id, start_time)
);

CREATE TABLE TimestampValueModelTypes(
    timestampValueModelShort smallint,
    timestampModel varchar(500),
    valueModel varchar(500)
);

INSERT INTO TimestampValueModelTypes(timestampValueModelShort, valuemodel, timestampmodel) VALUES
    (0, 'PMC-Mean', 'Regular'),
    (1, 'PMC-Mean', 'Delta Delta'),
    (2, 'PMC-Mean', 'SI-Difference');

INSERT INTO TimestampValueModelTypes(timestampValueModelShort, valuemodel, timestampmodel) VALUES
    (256, 'Swing', 'Regular'),
    (257, 'Swing', 'Delta Delta'),
    (258, 'Swing', 'SI-Difference');

INSERT INTO TimestampValueModelTypes(timestampValueModelShort, valuemodel, timestampmodel) VALUES
    (512, 'Gorilla', 'Regular'),
    (513, 'Gorilla', 'Delta Delta'),
    (514, 'Gorilla', 'SI-Difference');


INSERT INTO TimestampValueModelTypes(timestampValueModelShort, valuemodel, timestampmodel) VALUES
    (771, 'Fallback', 'Fallback');

CREATE TYPE sqlDataPoint AS(timeSeriesId integer, timestamp BigInt, value float);

CREATE FUNCTION decompressSegment(segment)
    RETURNS Setof sqlDataPoint AS 'SegmentDecompressor.decompressSegment'
    IMMUTABLE LANGUAGE java
;
