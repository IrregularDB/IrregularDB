package compression;

import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.List;

public abstract class BaseModel<E> {
    private final double errorBound;

    public BaseModel(double errorBound) {
        this.errorBound = errorBound;
    }

    public double getErrorBound() {
        return errorBound;
    }

    protected abstract void resetModel();

    public abstract int getLength();

    /**
     * Is used to reset the model and append a series of data points to it
     * often used right after emitting a segment to fill it with the buffer again
     * @return returns true if the entire time stamp list could be appended
     */
    public final boolean resetAndAppendAll(List<DataPoint> input) {
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

    public abstract boolean append(DataPoint dataPoint);

    public final double getCompressionRatio() {
        int amtDataPoints = this.getLength();
        // We get the size of the BLOB by reading its position, which indicates how many bytes we have used
        int amtBytesUsed = this.getBlobRepresentation().position();

        return (double)amtDataPoints/ (double)(amtBytesUsed);
    }

    /**
     * Byte list representation of the model that should be saved to the database
     * @return ByteBuffer class from java
     */
    public abstract ByteBuffer getBlobRepresentation();

    /**
     * This method is used when joining time stamp and value compression models
     *  by reducing the longest of the two down to the length of the other one
     */
    public abstract void reduceToSizeN(int n);

}
