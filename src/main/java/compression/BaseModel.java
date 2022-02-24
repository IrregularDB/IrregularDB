package compression;

import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Function;

public abstract class BaseModel<E> {
    private final Double errorBound;
    private final Integer lengthBound;

    public BaseModel(Double errorBound, Integer lengthBound) {
        // This small hack is added because floating point imprecision can lead to error-bound
        // of zero not really working.
        if (errorBound != null) {
            if (errorBound == 0) {
                this.errorBound = 0.00001;
            } else {
                this.errorBound = errorBound;
            }
        } else {
            this.errorBound = null;
        }
        this.lengthBound = lengthBound;
    }

    public double getErrorBound() {
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

    /**
     *
     * @return greater value represents better compression
     */
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
