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
    private final int AMT_TO_BUFFER = 100;

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
        if (insertBuffer.size() < AMT_TO_BUFFER) {
            return;
        }
        try {

            final String INSERT_SEGMENT_STATEMENT = "INSERT INTO Segment(time_series_id, start_time, end_time, value_timestamp_model_type, value_model_blob, timestamp_model_blob) VALUES (?,?,?,?,?,?)";
            PreparedStatement preparedStatement = connection.prepareStatement(INSERT_SEGMENT_STATEMENT, Statement.RETURN_GENERATED_KEYS);

            for (Pair<Segment, SegmentSummary> segmentSegmentSummaryPair : insertBuffer) {
                getPreparedStatementForInsertSegment(segmentSegmentSummaryPair.f0(), preparedStatement);
                preparedStatement.addBatch();
                preparedStatement.clearParameters();
            }

            preparedStatement.executeBatch();

//            if (segmentSummary != null) {
//                insertSegmentSummary(segmentSummary, insertSegmentStatement.getGeneratedKeys());
//            }

            insertBuffer.clear();
        } catch (SQLException e) {
            System.out.println("Couldn't insert segment: " + segment.toString() + "\n\n" + e.getMessage());
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

}
