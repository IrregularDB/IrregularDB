import org.postgresql.pljava.annotation.Function;
import records.Segment;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class UDFs {
    @Function
    public static Iterator<String> getTimeSeries(String tag) throws SQLException {
        Statement statement = DriverManager
                .getConnection("jdbc:default:connection")
                .createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT * FROM segments WHERE  = segmentId");

        while (resultSet.next()){
            int id = resultSet.getInt(0);
            long startTime = resultSet.getLong(1);
            int endTime = resultSet.getInt(2);
            byte type = resultSet.getByte(3);
            byte[] encodedValues = resultSet.getBytes(4);
            byte[] encodedTimestamps = resultSet.getBytes(5);

            // TODO: ValueCompressionModelType valueType = type
            // TODO: TimeStampCompressionModelType timestampType = type

            Segment segment = new Segment(
                    id,
                    startTime,
                    endTime,
                    null,
                    encodedValues,
                    null,
                    encodedTimestamps
            );
            resultSet.getRow();
        }

        List<String> dataPoints = new ArrayList<>();
        return
    }
}
