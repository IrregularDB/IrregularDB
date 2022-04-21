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
            flushBatchToDB();
        }
    }

    private void prepareStatementForInsertSegmentSummary(Pair<Segment, SegmentSummary> segmentAndSummaryPair, PreparedStatement preparedStatement) throws SQLException {
        Segment segment = segmentAndSummaryPair.f0();
        SegmentSummary segmentSummary = segmentAndSummaryPair.f1();

        preparedStatement.setInt(1, segment.timeSeriesId());
        preparedStatement.setLong(2, segment.startTime());
        preparedStatement.setFloat(3, segmentSummary.getMinValue());
        preparedStatement.setFloat(4, segmentSummary.getMaxValue());
        preparedStatement.setInt(5, segmentSummary.getAmtDataPoints());
    }

    private void prepareStatementForInsertSegment(Segment segment, PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setInt(1, segment.timeSeriesId());
        preparedStatement.setLong(2, segment.startTime());
        preparedStatement.setInt(3, (int) (segment.endTime() - segment.startTime()));
        preparedStatement.setShort(4, ModelTypeUtil.combineTwoModelTypes(segment.valueModelType(), segment.timestampModelType())); // we are now combining the two model types
        preparedStatement.setBytes(5, segment.valueBlob().array());
        preparedStatement.setBytes(6, segment.timestampBlob().array());
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
    public void flushBatchToDB() {
        try {
            final String INSERT_SEGMENT_STATEMENT = "INSERT INTO Segment(time_series_id, start_time, end_time, value_timestamp_model_type, value_model_blob, timestamp_model_blob) VALUES (?,?,?,?,?,?)";
            PreparedStatement insertSegmentStatement = connection.prepareStatement(INSERT_SEGMENT_STATEMENT, Statement.RETURN_GENERATED_KEYS);

            final String INSERT_SEGMENT_SUMMARY_STATEMENT = "INSERT INTO SegmentSummary(time_series_id, start_time, minValue, maxvalue, amtDataPoints) VALUES (?,?,?,?,?)";
            PreparedStatement insertSegmentSummaryStatement = connection.prepareStatement(INSERT_SEGMENT_SUMMARY_STATEMENT);

            boolean anySegmentSummaryUsed = false;
            for (Pair<Segment, SegmentSummary> segmentSegmentSummaryPair : insertBuffer) {
                prepareStatementForInsertSegment(segmentSegmentSummaryPair.f0(), insertSegmentStatement);
                insertSegmentStatement.addBatch();
                insertSegmentStatement.clearParameters();

                if (segmentSegmentSummaryPair.f1() != null) { // Handling of summary
                    prepareStatementForInsertSegmentSummary(segmentSegmentSummaryPair, insertSegmentSummaryStatement);
                    insertSegmentSummaryStatement.addBatch();
                    insertSegmentSummaryStatement.clearParameters();
                    anySegmentSummaryUsed = true;
                }
            }

            insertSegmentStatement.executeBatch();
            if (anySegmentSummaryUsed) {
                insertSegmentSummaryStatement.executeBatch();
            }
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
                                                         minValue real,
                                                         maxValue real,
                                                         amtDataPoints int,
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
