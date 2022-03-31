import compression.BlobDecompressor;
import compression.timestamp.TimeStampCompressionModelType;
import compression.utility.ModelTypeUtil;
import compression.value.ValueCompressionModelType;
import org.postgresql.pljava.ResultSetProvider;
import records.DataPoint;
import records.ValueTimeStampModelPair;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SegmentDecompressor implements ResultSetProvider {

    private List<DataPoint> readings;
    private final int timeSeriesId;

    public SegmentDecompressor(int timeSeriesId, long startTime, int endTime, short valueTimestampModelType,
                               byte[] valueModelBlob, byte[] timestampModelBlob) {

        ValueTimeStampModelPair valueTimeStampModelPair = ModelTypeUtil.combinedModelTypesToIndividual(valueTimestampModelType);

        this.timeSeriesId = timeSeriesId;
        this.readings = BlobDecompressor.decompressBlobs(
                TimeStampCompressionModelType.values()[valueTimeStampModelPair.timeStampModelType()],
                ByteBuffer.wrap(timestampModelBlob),
                ValueCompressionModelType.values()[valueTimeStampModelPair.valueModelType()],
                ByteBuffer.wrap(valueModelBlob),
                startTime,
                (long) (startTime + endTime));
    }

    @Override
    public boolean assignRowValues(ResultSet resultSet, int i) throws SQLException {
        if (this.readings.size() > i) {
            DataPoint dataPoint = this.readings.get(i);
            resultSet.updateInt(1, this.timeSeriesId);
            resultSet.updateLong(2, dataPoint.timestamp());
            resultSet.updateFloat(3, dataPoint.value());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() throws SQLException {
        this.readings = null;
    }

    public static ResultSetProvider decompressSegment(
            ResultSet resultSet
//            int timeSeriesId, long startTime, int endTime, short valueTimestampModelType,
//                                                    byte[] valueModelBlob, byte[] timestampModelBlob
    ) throws SQLException
    {
        return new SegmentDecompressor(resultSet.getInt(1), resultSet.getLong(2), resultSet.getInt(3), resultSet.getShort(4), resultSet.getBytes(5), resultSet.getBytes(6));
    }
}
