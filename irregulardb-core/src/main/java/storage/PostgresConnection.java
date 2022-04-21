package storage;

import compression.utility.ModelTypeUtil;
import config.ConfigProperties;
import records.Pair;
import records.Segment;
import records.SegmentSummary;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostgresConnection implements DatabaseConnection {


    private final Connection connection;
    private final List<Pair<Segment, SegmentSummary>> insertBuffer;
    private final int BATCH_SIZE = ConfigProperties.getInstance().getJDBCBatchSize();

    public PostgresConnection() {
        try {
            // Instantiate database connection
            // jdbc:postgresql://localhost/test?user=fred&password=secret
            ConfigProperties configProperties = ConfigProperties.getInstance();
            this.connection = DriverManager.getConnection(configProperties.getJDBConnectionString());
            this.insertBuffer = new ArrayList<>();
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void insertSegment(Segment segment, SegmentSummary segmentSummary) {
        insertBuffer.add(new Pair<>(segment, segmentSummary));
        if (insertBuffer.size() < BATCH_SIZE) {
            return;
        } else {
            flushCache();
        }
    }

    private void insertSegmentSummary(SegmentSummary segmentSummary, ResultSet generatedKeys) throws SQLException {
        if (generatedKeys.next()) {
            int time_series_id = generatedKeys.getInt("time_series_id");
            long start_time = generatedKeys.getLong("start_time");

            PreparedStatement preparedStatementForInsertSegmentSummary = getPreparedStatementForInsertSegmentSummary(time_series_id, start_time, segmentSummary);
            preparedStatementForInsertSegmentSummary.execute();
        }
    }

    private PreparedStatement getPreparedStatementForInsertSegmentSummary(int timeSeriedId, long startTime, SegmentSummary segmentSummary) throws SQLException {
        final String INSERT_SEGMENT_SUMMARY_STATEMENT = "INSERT INTO SegmentSummary(time_series_id, start_time, minValue, maxvalue) VALUES (?,?,?,?)";
        PreparedStatement preparedStatement = connection.prepareStatement(INSERT_SEGMENT_SUMMARY_STATEMENT);
        preparedStatement.setInt(1, timeSeriedId);
        preparedStatement.setLong(2, startTime);
        preparedStatement.setFloat(3, segmentSummary.getMinValue());
        preparedStatement.setFloat(4, segmentSummary.getMaxValue());
        return preparedStatement;
    }

    private PreparedStatement getPreparedStatementForInsertSegment(Segment segment, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(1, segment.timeSeriesId());
        preparedStatement.setLong(2, segment.startTime());
        preparedStatement.setInt(3, (int) (segment.endTime() - segment.startTime()));
        preparedStatement.setShort(4, ModelTypeUtil.combineTwoModelTypes(segment.valueModelType(), segment.timestampModelType())); // we are now combining the two model types
        preparedStatement.setBytes(5, segment.valueBlob().array());
        preparedStatement.setBytes(6, segment.timestampBlob().array());
        return preparedStatement;
    }


    @Override
    public int getTimeSeriesId(String timeSeriesTag) {
        final String GET_TIME_SERIES_BY_TAG = "SELECT * FROM TimeSeries where tag = ?";
        final String INSERT_TIME_SERIES_ID = "INSERT INTO TimeSeries (tag) VALUES (?)";

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(GET_TIME_SERIES_BY_TAG);
            preparedStatement.setString(1, timeSeriesTag);
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getInt("id");
            }

            PreparedStatement st = connection.prepareStatement(INSERT_TIME_SERIES_ID, Statement.RETURN_GENERATED_KEYS);
            st.setString(1, timeSeriesTag);
            st.execute();

            ResultSet generatedKeys = st.getGeneratedKeys();
            generatedKeys.next();
            int timeSeriesId = generatedKeys.getInt(1);
            preparedStatement.close();
            resultSet.close();

            return timeSeriesId;
        } catch (SQLException e) {
            System.out.println("Couldn't insert time series for tag: " + timeSeriesTag + "\n\n" + e.getMessage());
        }
        return -1;
    }

    @Override
    public void flushCache() {
        try {
            final String INSERT_SEGMENT_STATEMENT = "INSERT INTO Segment(time_series_id, start_time, end_time, value_timestamp_model_type, value_model_blob, timestamp_model_blob) VALUES (?,?,?,?,?,?)";
            PreparedStatement preparedStatement = connection.prepareStatement(INSERT_SEGMENT_STATEMENT, Statement.RETURN_GENERATED_KEYS);

            for (Pair<Segment, SegmentSummary> segmentSegmentSummaryPair : insertBuffer) {
                getPreparedStatementForInsertSegment(segmentSegmentSummaryPair.f0(), preparedStatement);
                preparedStatement.addBatch();
                preparedStatement.clearParameters();
            }

            preparedStatement.executeBatch();
            /*if (segmentSummary != null) {
                   insertSegmentSummary(segmentSummary, insertSegmentStatement.getGeneratedKeys());
            }*/
            insertBuffer.clear();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void resetTables() throws SQLException {
        final String SQL = """
                DROP TABLE IF EXISTS TimestampValueModelTypes;
                DROP TABLE IF EXISTS SegmentSummary;
                DROP TABLE IF EXISTS Segment;
                                
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
                    minValue real,
                    maxValue real,
                    CONSTRAINT fk_segmentSummary_ts_key_to_segment
                        FOREIGN KEY(time_series_id, start_time) REFERENCES Segment(time_series_id, start_time),
                    CONSTRAINT pk_segmentSummary_timeId_startTime
                        PRIMARY KEY (time_series_id, start_time)
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
                """;


        Connection connection = DriverManager.getConnection(ConfigProperties.getInstance().getJDBConnectionString());
        Statement statement = connection.createStatement();
        statement.execute(SQL);
    }
}
