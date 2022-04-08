package compression;

import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.List;

public abstract class BaseModel {
    private final String cantConstructBlobErrorMessage;

    private ByteBuffer byteBuffer; //Used as a cache for a blob

    public BaseModel(String cantConstructBlobErrorMessage) {
        this.cantConstructBlobErrorMessage = cantConstructBlobErrorMessage;
        byteBuffer = null;
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
        return getBlobRepresentation().capacity();
    }

    /**
     * Byte list representation of the model that should be saved to the database
     * @return ByteBuffer class from java
     */
    public final ByteBuffer getBlobRepresentation() {
        if (!canCreateByteBuffer()) {
            throw new IllegalStateException(cantConstructBlobErrorMessage);
        }
        if (byteBuffer == null) {
            byteBuffer = createByteBuffer();
        }
        return byteBuffer;
    }

    protected abstract ByteBuffer createByteBuffer();


    public abstract boolean canCreateByteBuffer();

    /**
     * This method is used when joining time stamp and value compression models
     *  by reducing the longest of the two down to the length of the other one
     */
    public final void reduceToSizeN(int n) {
        if (n == this.getLength()) {
            return;
        }else if (n <= 0){
            throw new IllegalArgumentException("n cannot be 0 or lower");
        } else if (n > this.getLength()){
            throw new IllegalArgumentException("n cannot bigger than amount of elements represented by model");
        }
        this.byteBuffer = null;
        reduceToSize(n);
    }

    protected abstract void reduceToSize(int n);
}
