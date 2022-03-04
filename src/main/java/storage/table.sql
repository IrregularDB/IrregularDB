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
    end_time bigint not null,
    value_model_type int2 not null,
    value_model_blob bytea not null,
    timestamp_model_type int2 not null,
    timestamp_model_blob bytea not null,
    CONSTRAINT fk_time_series
                    FOREIGN KEY(time_series_id)
                    REFERENCES TimeSeries(id)
                    ON DELETE CASCADE,
    CONSTRAINT pk_timeId_startTime primary key(time_series_id, start_time)
);
