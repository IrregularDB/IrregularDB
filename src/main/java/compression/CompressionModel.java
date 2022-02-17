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

    ByteBuffer getValueBlob() {
        return valueCompressionModel.getValueBlob();
    }

    ByteBuffer getTimeStampBlob() {
        return timeStampCompressionModel.getTimeStampBlob();
    }

    ValueCompressionModelType getValueCompressionModelType() {
        return valueCompressionModel.getValueCompressionModelType();
    }

    TimeStampCompressionModelType getTimeStampCompressionModelType() {
        return timeStampCompressionModel.getTimeStampCompressionModelType();
    }
}
