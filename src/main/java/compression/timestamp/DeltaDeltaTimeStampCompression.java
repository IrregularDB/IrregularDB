package compression.timestamp;

import records.DataPoint;

import java.nio.ByteBuffer;

public class DeltaDeltaTimeStampCompression extends TimeStampCompressionModel{



    public DeltaDeltaTimeStampCompression(Float errorBound, Integer lengthBound) {
        super(errorBound, lengthBound);
    }

    @Override
    protected void resetModel() {

    }

    @Override
    public int getLength() {
        return 0;
    }

    @Override
    protected boolean appendDataPoint(DataPoint dataPoint) {
        return false;
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        return null;
    }

    @Override
    protected void reduceToSize(int n) {

    }

    @Override
    public TimeStampCompressionModelType getTimeStampCompressionModelType() {
        return null;
    }
}
