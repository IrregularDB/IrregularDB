package compression.value;

import compression.encoding.GorillaValueEncoding;
import compression.utility.BitBuffer;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class GorillaValueCompressionModel extends ValueCompressionModel {
    List<Float> values;

    public GorillaValueCompressionModel(int lengthBound) {
        super(null, lengthBound);
        this.resetModel();
    }

    @Override
    protected void resetModel() {
        values = new ArrayList<>();
    }

    @Override
    public int getLength() {
        return values.size();
    }

    @Override
    protected boolean appendDataPoint(DataPoint dataPoint) {
        if (this.getLength() < super.getLengthBound()) {
            values.add(dataPoint.value());
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        BitBuffer encode = GorillaValueEncoding.encode(values);
        return encode.getByteBuffer();
    }

    @Override
    protected void reduceToSize(int n) {
        int length = this.getLength();
        if (length < n) {
            throw new IllegalArgumentException("You tried to reduce this size of a model to something smaller than its current size");
        }
        values.subList(n, this.getLength()).clear();
    }

    @Override
    public ValueCompressionModelType getValueCompressionModelType() {
        return ValueCompressionModelType.GORILLA;
    }
}
