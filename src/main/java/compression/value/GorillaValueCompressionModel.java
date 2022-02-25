package compression.value;

import compression.encoding.Encoding;
import compression.encoding.GorillaValueEncoder;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class GorillaValueCompressionModel extends ValueCompressionModel {
    Encoding<Float> valueEncoder;
    List<Float> values;
    // Helper field used to make it so that we don't reconstruct the byte buffer unnecessary times
    private int amtValuesRepresentedByCurrentByteBuffer;
    private ByteBuffer byteBuffer;

    public GorillaValueCompressionModel(int lengthBound) {
        super(null, lengthBound);
        this.valueEncoder = new GorillaValueEncoder();
        this.resetModel();
    }

    @Override
    protected void resetModel() {
        values = new ArrayList<>();
        byteBuffer = null;
        amtValuesRepresentedByCurrentByteBuffer = -1;
    }

    @Override
    public int getLength() {
        return values.size();
    }

    @Override
    public boolean append(DataPoint dataPoint) {
        if (this.getLength() < super.getLengthBound()) {
            values.add(dataPoint.value());
            return true;
        } else {
            return false;
        }
    }

    @Override
    public ByteBuffer getBlobRepresentation() {
        int length = this.getLength();
        if (length != amtValuesRepresentedByCurrentByteBuffer) {
            this.amtValuesRepresentedByCurrentByteBuffer = length;
            this.byteBuffer = valueEncoder.encode(values).getByteBuffer();
        }
        return byteBuffer;
    }

    @Override
    public void reduceToSizeN(int n) {
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
