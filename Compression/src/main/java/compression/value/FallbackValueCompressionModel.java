package compression.value;

import compression.timestamp.TimestampCompressionModelType;
import records.DataPoint;

import java.nio.ByteBuffer;

public class FallbackValueCompressionModel extends ValueCompressionModel {
    private final Float value;

    public FallbackValueCompressionModel(float value) {
        super(0f, null, false, -1);
        this.value = value;
    }

    @Override
    public ValueCompressionModelType getValueCompressionModelType() {
        return ValueCompressionModelType.FALLBACK;
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(4);
        byteBuffer.putFloat(value);
        return byteBuffer;
    }

    @Override
    public boolean canCreateByteBuffer() {
        return value != null;
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
