package compression;

import compression.timestamp.TimeStampCompressionModel;
import compression.timestamp.TimeStampCompressionModelType;
import compression.value.ValueCompressionModel;
import compression.value.ValueCompressionModelType;
import records.DataPoint;

import java.nio.ByteBuffer;

public class CompressionModel {
    private final ValueCompressionModel valueCompressionModel;
    private final TimeStampCompressionModel timeStampCompressionModel;

    public CompressionModel(ValueCompressionModel valueCompressionModel, TimeStampCompressionModel timeStampCompressionModel) {
        this.valueCompressionModel = valueCompressionModel;
        this.timeStampCompressionModel = timeStampCompressionModel;
    }

    public boolean appendDataPoint(DataPoint dataPoint) {
        boolean valueAppendSucceeded = this.valueCompressionModel.appendValue(dataPoint.value());
        boolean timeStampAppendSuceeded = this.timeStampCompressionModel.appendTimeStamp(dataPoint.timestamp());

        return valueAppendSucceeded && timeStampAppendSuceeded;
    }

    public ByteBuffer getValueBlob() {
        return valueCompressionModel.getValueBlob();
    }

    public ByteBuffer getTimeStampBlob() {
        return timeStampCompressionModel.getTimeStampBlob();
    }

    public ValueCompressionModelType getValueCompressionModelType() {
        return valueCompressionModel.getValueCompressionModelType();
    }

    public TimeStampCompressionModelType getTimeStampCompressionModelType() {
        return timeStampCompressionModel.getTimeStampCompressionModelType();
    }
}
