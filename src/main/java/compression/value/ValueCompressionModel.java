package compression.value;
import records.DataPoint;

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
        this.resetModel();
        this.errorBound = errorBound;
    }

    // Remember to override this method with an extension where you reset the model state
    protected void resetModel() {
        this.values = new ArrayList<>();
    }

    public int getLength() {
        return values.size();
    }

    public abstract boolean appendValue(double value);

    /**
     * Is used to reset the model and append a series of data points to it
     * often used right after emitting a segment to fill it with the buffer again
     * @return returns true if the entire value list could be appended
     */
    public boolean resetAndAppendAll(List<Double> values) {
        this.resetModel();

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

    public double getCompressionRatio() {
        throw new RuntimeException("Not tested but idea for implementation in abstract class");
        // THE BYTE BUFFER HAS NOT SIZE SO WE NEED TO DO SOMETHING ELSE
        //return (double)this.getLength() / (double)(this.getValueBlob().position());
    }

    /**
     * This method is used when joining time stamp and value compression models
     *  by reducing the longest of the two down to the length of the other one
     */
    public abstract void reduceToSizeN(int n);
}
