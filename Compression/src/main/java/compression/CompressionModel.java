package compression;

import compression.timestamp.TimestampCompressionModel;
import compression.timestamp.TimestampCompressionModelType;
import compression.value.ValueCompressionModel;
import compression.value.ValueCompressionModelType;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.List;

public class CompressionModel {
    private final ValueCompressionModel valueCompressionModel;
    private final TimestampCompressionModel timestampCompressionModel;

    public CompressionModel(ValueCompressionModel valueCompressionModel, TimestampCompressionModel timeStampCompressionModel) {
        this.valueCompressionModel = valueCompressionModel;
        this.timestampCompressionModel = timeStampCompressionModel;
    }

    public boolean appendDataPoint(DataPoint dataPoint) {
        boolean valueAppendSucceeded = this.valueCompressionModel.append(dataPoint);
        boolean timestampAppendSucceeded = this.timestampCompressionModel.append(dataPoint);

        return valueAppendSucceeded && timestampAppendSucceeded;
    }

    public ByteBuffer getValueBlob() {
        return valueCompressionModel.getBlobRepresentation();
    }

    public ByteBuffer getTimestampBlob() {
        return timestampCompressionModel.getBlobRepresentation();
    }

    public ValueCompressionModelType getValueCompressionModelType() {
        return valueCompressionModel.getValueCompressionModelType();
    }

    public TimestampCompressionModelType getTimestampCompressionModelType() {
        return timestampCompressionModel.getTimestampCompressionModelType();
    }

    public boolean resetModel(List<DataPoint> dataPoints){
        return this.valueCompressionModel.resetAndAppendAll(dataPoints) &&
                this.timestampCompressionModel.resetAndAppendAll(dataPoints);
    }

    public ValueCompressionModel getValueCompressionModel() {
        return valueCompressionModel;
    }

    public TimestampCompressionModel getTimestampCompressionModel() {
        return timestampCompressionModel;
    }
}
