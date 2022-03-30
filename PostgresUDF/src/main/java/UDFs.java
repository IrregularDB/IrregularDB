import org.postgresql.pljava.annotation.Function;
import records.Segment;

import javax.sql.rowset.CachedRowSet;
import java.nio.ByteBuffer;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class UDFs {

//    @Function
//    public static Iterator<String> getTimeSeries(String tag) throws SQLException {
//        Statement statement = DriverManager
//                .getConnection("jdbc:default:connection")
//                .createStatement();
//        ResultSet resultSet = statement.executeQuery("SELECT * FROM segments WHERE  = segmentId");
//
//        while (resultSet.next()){
//            int id = resultSet.getInt(0);
//            long startTime = resultSet.getLong(1);
//            int endTime = resultSet.getInt(2);
//            byte type = resultSet.getByte(3);
//            byte[] encodedValues = resultSet.getBytes(4);
//            byte[] encodedTimestamps = resultSet.getBytes(5);
//
//            // TODO: ValueCompressionModelType valueType = type
//            // TODO: TimeStampCompressionModelType timestampType = type
//
//            Segment segment = new Segment(
//                    id,
//                    startTime,
//                    endTime,
//                    null,
//                    encodedValues,
//                    null,
//                    encodedTimestamps
//            );
//            resultSet.getRow();
//        }
//
//        List<String> dataPoints = new ArrayList<>();
//        return
//    }

    @Function
    public static boolean decompress(
//            int timeSeriesId, long startTime, int endTime,
//                                                short valueTimestampModelType, byte[] valueModelBlob,
//                                                byte[] timestampModelBlob,
            ResultSet outResultSet) throws SQLException {
//        ModelTypeUtil.ValueTimeStampModelPair valueTimeStampModelPair = ModelTypeUtil.combinedModelTypesToIndividual(valueTimestampModelType);
//
//        List<DataPoint> decompressedDataPoints = BlobDecompressor.decompressBlobs(
//                // WHAT IS THIS?!
//                TimeStampCompressionModelType.values()[valueTimeStampModelPair.timeStampModelType()],
//                ByteBuffer.wrap(timestampModelBlob),
//                ValueCompressionModelType.values()[valueTimeStampModelPair.valueModelType()],
//                ByteBuffer.wrap(valueModelBlob),
//                startTime,
//                (long) (startTime + endTime));

//        List<String[]> result = new ArrayList<>();
//        for (int i = 0; i < decompressedDataPoints.size(); i++) {
//
//        }

//        CachedRowSet cachedRowSet = new CachedRowSet();
//        String[] a1 = {"A","B"};
//        String[] myArray = {"w", "e"};

        outResultSet.updateInt(1, -1);
        outResultSet.updateLong(2, -1);
        outResultSet.updateFloat(3, -1);
//        return List.of(new SQLDataPoint(1,1,1), new SQLDataPoint(1,2,2)).iterator();

        return true;
//        return List.of(a1, myArray).iterator();
    }
}