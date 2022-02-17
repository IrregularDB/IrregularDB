package compression.value;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

enum ValueCompressionModelType {
    PMCMEAN,
    SWING,
    GORILLA
}


public abstract class ValueCompressionModel {
    protected double errorBound;
    protected List<Double> values;

    public ValueCompressionModel(double errorBound) {
        this.resetEntireModel();
        this.errorBound = errorBound;
    }

    public final int getLength() {
        return values.size();
    }

    public abstract boolean appendValue(double value);

    protected abstract void resetModelParameters();

    protected final void resetEntireModel() {
        this.values = new ArrayList<>();
        this.resetModelParameters();
    }

    /**
     * Is used to reset the model and append a series of data points to it
     * often used right after emitting a segment to fill it with the buffer again
     * @return returns true if the entire value list could be appended
     */
    public final boolean resetAndAppendAll(List<Double> values) {
        this.resetEntireModel();

        boolean appendSucceeded = false;
        for (double value : values) {
            appendSucceeded = this.appendValue(value);
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
    public abstract ByteBuffer getValueBlob();

    /**
     * Used to identify which type of model is used when writing to the DB
     * @return enum for the value compression model types
     */
    public abstract ValueCompressionModelType getValueCompressionModelType();

    public final double getCompressionRatio() {
        // We get the size of the BLOB by reading its position, which indicates how many bytes we have used
        int valueBlobPosition = this.getValueBlob().position();
        return (double)this.getLength() / (double)(valueBlobPosition);
    }

    /**
     * This method is used when joining time stamp and value compression models
     *  by reducing the longest of the two down to the length of the other one
     */
    public abstract void reduceToSizeN(int n);
}
