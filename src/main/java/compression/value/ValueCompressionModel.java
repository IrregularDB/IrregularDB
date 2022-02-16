package compression.value;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.List;

enum ValueCompressionModelTypes {
    PMCMEAN,
    SWING,
    GORILLA
}


public interface ValueCompressionModel {
    boolean appendDatapoint(DataPoint dataPoint);
    boolean appendBuffer(List<DataPoint> dataPointBuffer);
    int getLength();
    double getCompressionRatio();
    ByteBuffer getValueBlob();
    ValueCompressionModelTypes getValueCompressionModelType();

    /**
     * This method is used when joining time stamp and value compression models
     *  by reducing the longest of the two down to the length of the other one
     */
    void reduceToSizeN(int n);
}
