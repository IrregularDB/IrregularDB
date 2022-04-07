package compression.value;

import compression.encoding.GorillaValueEncoding;
import compression.utility.BitBuffer.BitBuffer;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class GorillaValueCompressionModel extends ValueCompressionModel {
    List<Float> values;

    public GorillaValueCompressionModel() {
        super(null);
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
        values.add(dataPoint.value());
        return true;
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        if (!canCreateByteBuffer()) {
            throw new UnsupportedOperationException("No data points where added to the Gorilla value model before trying to get the value blob");
        }

        BitBuffer encode = GorillaValueEncoding.encode(values);
        return encode.getFinishedByteBuffer();
    }

    @Override
    public boolean canCreateByteBuffer() {
        return this.getLength() != 0;
    }

    @Override
    protected void reduceToSize(int n) {
        values.subList(n, this.getLength()).clear();
    }

    @Override
    public ValueCompressionModelType getValueCompressionModelType() {
        return ValueCompressionModelType.GORILLA;
    }
}
