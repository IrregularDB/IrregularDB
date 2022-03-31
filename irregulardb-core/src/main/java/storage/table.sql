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
    CONSTRAINT fk_time_series
                    FOREIGN KEY(time_series_id)
                    REFERENCES TimeSeries(id)
                    ON DELETE CASCADE,
    CONSTRAINT pk_segment_timeId_startTime primary key(time_series_id, start_time)
);

CREATE TABLE SegmentSummary(
    time_series_id int not null,
    start_time bigint not null,
    average real,
    CONSTRAINT fk_segmentSummary_ts_key_to_segment
        FOREIGN KEY(time_series_id, start_time) REFERENCES Segment(time_series_id, start_time),
    CONSTRAINT pk_segmentSummary_timeId_startTime
        PRIMARY KEY (time_series_id, start_time)
);

INSERT INTO SegmentSummary (time_series_id, start_time, average) VALUES (?,?,?);