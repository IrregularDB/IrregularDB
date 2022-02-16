package compression.timestamp;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.List;

enum TimeStampCompressionModelTypes {
    REGULAR,
    BASEDELTA,
    DELTAPAIRS,
    RECOMPUTESI
}

public interface TimeStampCompressionModel {
    boolean appendDatapoint(DataPoint dataPoint);
    boolean appendBuffer(List<DataPoint> dataPointBuffer);
    int getLength();
    double getCompressionRatio();
    ByteBuffer getValueBlob();
    TimeStampCompressionModel getValueCompressionModelType();

    /**
     * This method is used when joining time stamp and value compression models
     *  by reducing the longest of the two down to the length of the other one
     */
    void reduceToSizeN(int n);

}
