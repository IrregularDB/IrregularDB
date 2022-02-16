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
    protected List<DataPoint> dataPoints;

    public ValueCompressionModel(double errorBound) {
        this.errorBound = errorBound;
        this.dataPoints = new ArrayList<>();
    }

    public int getLength() {
        return dataPoints.size();
    }

    public double getCompressionRatio() {
        throw new RuntimeException("Not tested but idea for implementation in abstract class");
        // THE BYTE BUFFER HAS NOT SIZE SO WE NEED TO DO SOMETHING ELSE
        //return (double)this.getLength() / (double)(this.getValueBlob().position());
    }

    public abstract boolean appendDatapoint(DataPoint dataPoint);
    public abstract boolean appendBuffer(List<DataPoint> dataPointBuffer);

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

    /**
     * This method is used when joining time stamp and value compression models
     *  by reducing the longest of the two down to the length of the other one
     */
    public abstract void reduceToSizeN(int n);
}
