package compression.timestamp;

import records.DataPoint;

import java.nio.ByteBuffer;

public class FallbackTimeStampCompressionModel extends TimestampCompressionModel {
    private final Long timestamp;

    public FallbackTimeStampCompressionModel(Long timestamp) {
        super(-1, null, false, -1);
        this.timestamp = timestamp;
    }

    @Override
    public TimestampCompressionModelType getTimestampCompressionModelType() {
        return TimestampCompressionModelType.FALLBACK;
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        // We return an empty byte buffer as the start time is already located on the segment
        return ByteBuffer.allocate(0);
    }

    @Override
    public boolean canCreateByteBuffer() {
        return timestamp != null;
    }

    @Override
    protected void resetModel() {
        throw new RuntimeException("Not implemented as you should never have to reset the fall back model");
    }

    @Override
    public int getLength() {
        throw new RuntimeException("Not implemented as you should never get length on the fall back model");
    }

    @Override
    protected boolean appendDataPoint(DataPoint dataPoint) {
        throw new RuntimeException("Not implemented as you should never append to the fallback model");
    }

    @Override
    protected void reduceToSize(int n) {
        throw new RuntimeException("Not implemented as you should reduce to size n on the fallback model");
    }
}
