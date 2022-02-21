package compression;

import compression.timestamp.TimeStampCompressionModel;
import compression.timestamp.TimeStampCompressionModelType;
import compression.value.ValueCompressionModel;
import compression.value.ValueCompressionModelType;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.List;

public class CompressionModel {
    private final ValueCompressionModel valueCompressionModel;
    private final TimeStampCompressionModel timeStampCompressionModel;

    public CompressionModel(ValueCompressionModel valueCompressionModel, TimeStampCompressionModel timeStampCompressionModel) {
        this.valueCompressionModel = valueCompressionModel;
        this.timeStampCompressionModel = timeStampCompressionModel;
    }

    public boolean appendDataPoint(DataPoint dataPoint) {
        boolean valueAppendSucceeded = this.valueCompressionModel.append(dataPoint.value());
        boolean timeStampAppendSucceeded = this.timeStampCompressionModel.append(dataPoint.timestamp());

        return valueAppendSucceeded && timeStampAppendSucceeded;
    }

    public ByteBuffer getValueBlob() {
        return valueCompressionModel.getBlobRepresentation();
    }

    public ByteBuffer getTimeStampBlob() {
        return timeStampCompressionModel.getBlobRepresentation();
    }

    public ValueCompressionModelType getValueCompressionModelType() {
        return valueCompressionModel.getValueCompressionModelType();
    }

    public TimeStampCompressionModelType getTimeStampCompressionModelType() {
        return timeStampCompressionModel.getTimeStampCompressionModelType();
    }

    public boolean resetModel(List<DataPoint> dataPoints){
        return this.valueCompressionModel.resetAndAppendAll(dataPoints) &&
                this.timeStampCompressionModel.resetAndAppendAll(dataPoints);
    }

    public ValueCompressionModel getValueCompressionModel() {
        return valueCompressionModel;
    }

    public TimeStampCompressionModel getTimeStampCompressionModel() {
        return timeStampCompressionModel;
    }
}
