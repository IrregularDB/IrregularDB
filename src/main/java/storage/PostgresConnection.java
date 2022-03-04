package storage;

import compression.utility.BitUtil;
import config.ConfigProperties;
import records.Segment;

import java.sql.*;

public class PostgresConnection implements DatabaseConnection {

    private static final String INSERT_SEGMENT_STATEMENT = "INSERT INTO Segment(time_series_id, start_time, end_time, value_model_type, value_model_blob, timestamp_model_type, timestamp_model_blob) VALUES (?,?,?,?,?,?,?)";

    private static final String INSERT_TIME_SERIES_ID = "INSERT INTO TimeSeries (tag) VALUES (?)";
    private static final String GET_TIME_SERIES_BY_TAG = "SELECT * FROM TimeSeries where tag = ?";

    private final Connection connection;

    public PostgresConnection(){
        try {
            // Instantiate database connection
            // jdbc:postgresql://localhost/test?user=fred&password=secret
            ConfigProperties configProperties = ConfigProperties.INSTANCE;
            this.connection = DriverManager.getConnection(configProperties.getJDBConnectionString());
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public void insertSegment(Segment segment) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(INSERT_SEGMENT_STATEMENT);
            preparedStatement.setInt(1, segment.timeSeriesId());
            preparedStatement.setLong(2, segment.startTime());
            preparedStatement.setLong(3, segment.endTime());
            preparedStatement.setInt(4, BitUtil.combineTwoModelTypes(segment.valueModelType(), segment.timestampModelType())); // we are now combining the two model types
            preparedStatement.setBytes(5, segment.valueBlob().array());
            preparedStatement.setBytes(6, segment.timestampBlob().array());

            preparedStatement.execute();

            preparedStatement.close();
        } catch (SQLException e) {
            System.out.println("Couldn't insert segment: " + segment.toString() + "\n\n" + e.getMessage());
        }
    }


    @Override
    public int getTimeSeriesId(String timeSeriesTag) {

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
