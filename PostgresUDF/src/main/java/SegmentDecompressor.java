import compression.BlobDecompressor;
import compression.timestamp.TimestampCompressionModelType;
import compression.utility.ModelTypeUtil;
import compression.value.ValueCompressionModelType;
import org.postgresql.pljava.ResultSetProvider;
import records.DataPoint;
import records.ValueTimestampModelPair;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SegmentDecompressor implements ResultSetProvider {

    private List<DataPoint> readings;
    private final int timeSeriesId;

    public SegmentDecompressor(int timeSeriesId, long startTime, int endTime, short valueTimestampModelType,
                               byte[] valueModelBlob, byte[] timestampModelBlob) {

        ValueTimestampModelPair valueTimeStampModelPair = ModelTypeUtil.combinedModelTypesToIndividual(valueTimestampModelType);

        this.timeSeriesId = timeSeriesId;
        this.readings = BlobDecompressor.decompressBlobs(
                TimestampCompressionModelType.values()[valueTimeStampModelPair.timestampModelType()],
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
    //start_time bigint not null,
    //time_series_id int not null,
    //end_time int not null,
    //minValue real,
    //maxValue real,
    //value_timestamp_model_type int2 not null,
    //value_model_blob bytea not null,
    //timestamp_model_blob bytea not null

    public static ResultSetProvider decompressSegment(ResultSet resultSet) throws SQLException
    {
        return new SegmentDecompressor(resultSet.getInt(2), resultSet.getLong(1), resultSet.getInt(3), resultSet.getShort(6), resultSet.getBytes(7), resultSet.getBytes(8));
    }
}
