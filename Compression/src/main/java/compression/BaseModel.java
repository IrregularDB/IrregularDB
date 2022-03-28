package compression;

import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.List;

public abstract class BaseModel {
    private final Float errorBound;
    private final Integer lengthBound;
    private ByteBuffer byteBuffer; //Used as a cache for a blob

    /**
     * @param errorBound this value must be given as a percentage e.g. 10 not 0.1 for 10%;
     * @param lengthBound
     */
    public BaseModel(Float errorBound, Integer lengthBound) {
        // This small hack is added because floating point imprecision can lead to error-bound
        // of zero not really working.
        if (errorBound != null) {
            if (errorBound == 0) {
                this.errorBound = 0.00001F;
            } else {
                this.errorBound = errorBound / 100;
            }
        } else {
            this.errorBound = null;
        }
        this.lengthBound = lengthBound;
        byteBuffer = null;
    }

    public float getErrorBound() {
        if (errorBound == null) {
            throw new UnsupportedOperationException("You tried to get error bound for a model, which has no error bound defined");
        }
        return errorBound;
    }

    public int getLengthBound() {
        if (lengthBound == null) {
            throw new UnsupportedOperationException("You tried to get length bound for a model, which has no length bound defined");
        }
        return lengthBound;
    }

    protected abstract void resetModel();

    public abstract int getLength();

    /**
     * Is used to reset the model and append a series of data points to it
     * often used right after emitting a segment to fill it with the buffer again
     * @return returns true if the entire time stamp list could be appended
     */
    public final boolean resetAndAppendAll(List<DataPoint> input) {
        this.byteBuffer = null;
        this.resetModel();

        boolean appendSucceeded = true;
        for (DataPoint dataPoint : input) {
            appendSucceeded = this.append(dataPoint);
            if (!appendSucceeded) {
                break;
            }
        }
        return appendSucceeded;
    }

    public final boolean append(DataPoint dataPoint) {
        this.byteBuffer = null;
        return this.appendDataPoint(dataPoint);
    }

    protected abstract boolean appendDataPoint(DataPoint dataPoint);

    /**
     * @return returns amount of bytes used in the byte buffer to represent the model
     */
    public final int getAmountBytesUsed() {
        return getBlobRepresentation().position();
    }

    /**
     * Byte list representation of the model that should be saved to the database
     * @return ByteBuffer class from java
     */
    public final ByteBuffer getBlobRepresentation() {
        if (byteBuffer == null) {
            byteBuffer = createByteBuffer();
        }
        return byteBuffer;
    }

    protected abstract ByteBuffer createByteBuffer();

    /**
     * This method is used when joining time stamp and value compression models
     *  by reducing the longest of the two down to the length of the other one
     */
    public final void reduceToSizeN(int n) {
        if (n <= 0){
            throw new IllegalArgumentException("n cannot be 0 or lower");
        } else if (n > this.getLength()){
            throw new IllegalArgumentException("n cannot bigger than amount of elements represented by model");
        }
        this.byteBuffer = null;
        reduceToSize(n);
    }

    protected abstract void reduceToSize(int n);
}
