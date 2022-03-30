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
import java.util.stream.Collectors;

public class SetOfComplexTypeUDF implements ResultSetProvider {

    private List<TidDataPoint> readings;

    public SetOfComplexTypeUDF(int timeSeriesId, long startTime, int endTime, short valueTimestampModelType,
                               byte[] valueModelBlob, byte[] timestampModelBlob) {

        ValueTimeStampModelPair valueTimeStampModelPair = ModelTypeUtil.combinedModelTypesToIndividual(valueTimestampModelType);

        List<DataPoint> dataPoints = BlobDecompressor.decompressBlobs(
                TimeStampCompressionModelType.values()[valueTimeStampModelPair.timeStampModelType()],
                ByteBuffer.wrap(timestampModelBlob),
                ValueCompressionModelType.values()[valueTimeStampModelPair.valueModelType()],
                ByteBuffer.wrap(valueModelBlob),
                startTime,
                (long) (startTime + endTime)
        );

        readings = dataPoints.stream()
                .map(dp -> new TidDataPoint(timeSeriesId, dp.timestamp(), dp.value()))
                .collect(Collectors.toList());
    }

    @Override
    public boolean assignRowValues(ResultSet resultSet, int i) throws SQLException {
        if (this.readings.size() > i) {
            TidDataPoint tidDataPoint = this.readings.get(i);
            resultSet.updateInt(1, tidDataPoint.getTimeseriesId());
            resultSet.updateLong(2, tidDataPoint.getTimestamp());
            resultSet.updateFloat(3, tidDataPoint.getValue());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void close() throws SQLException {
        this.readings = null;
    }

    public static ResultSetProvider listComplexTest(int timeSeriesId, long startTime, int endTime, short valueTimestampModelType,
                                                    byte[] valueModelBlob, byte[] timestampModelBlob) throws SQLException
    {
        return new SetOfComplexTypeUDF(timeSeriesId, startTime, endTime, valueTimestampModelType, valueModelBlob, timestampModelBlob);
    }

    public static class TidDataPoint{
        private int timeseriesId;
        private long timestamp;
        private float value;

        public TidDataPoint(int timeseriesId, long timestamp, float value) {
            this.timeseriesId = timeseriesId;
            this.timestamp = timestamp;
            this.value = value;
        }

        public int getTimeseriesId() {
            return timeseriesId;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public float getValue() {
            return value;
        }
    }
}
