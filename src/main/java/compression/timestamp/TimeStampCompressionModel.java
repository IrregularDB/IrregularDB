package compression.timestamp;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class TimeStampCompressionModel {
    protected double errorBound;
    protected List<Long> timeStamps;

    public TimeStampCompressionModel(double errorBound) {
        this.resetEntireModel();
        this.errorBound = errorBound;
    }

    protected abstract void resetModelParameters();

    protected final void resetEntireModel() {
        this.timeStamps = new ArrayList<>();
        this.resetModelParameters();
    }

    public final int getLength() {
        return timeStamps.size();
    }

    public abstract boolean appendTimeStamp(long timeStamp);

    /**
     * Is used to reset the model and append a series of data points to it
     * often used right after emitting a segment to fill it with the buffer again
     * @return returns true if the entire time stamp list could be appended
     */
    public final boolean resetAndAppendAll(List<Long> timeStamps) {
        this.resetEntireModel();

        boolean appendSucceeded = true;
        for (Long timeStamp : timeStamps) {
            appendSucceeded = this.appendTimeStamp(timeStamp);
            if (!appendSucceeded) {
                break;
            }
        }
        return appendSucceeded;
    }

    /**
     * Byte list representation of the model that should be saved to the database
     * @return ByteBuffer class from java
     */
    public abstract ByteBuffer getTimeStampBlob();

    /**
     * Used to identify which type of model is used when writing to the DB
     * @return enum for the value compression model types
     */
    public abstract TimeStampCompressionModelType getTimeStampCompressionModelType();

    public final double getCompressionRatio() {
        // We get the size of the BLOB by reading its position, which indicates how many bytes we have used
        int amtDataPoints = this.getLength();
        int amtBytesUsed = this.getTimeStampBlob().position();

        return (double)amtDataPoints/ (double)(amtBytesUsed);
    }

    /**
     * This method is used when joining time stamp and value compression models
     *  by reducing the longest of the two down to the length of the other one
     */
    public abstract void reduceToSizeN(int n);
}

