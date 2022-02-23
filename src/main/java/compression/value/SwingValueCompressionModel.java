// The implementation of this model type is based on code
// published in relation to ModelarDB
// LINK: https://github.com/skejserjensen/ModelarDB

package compression.value;

import compression.utility.LinearFunction;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SwingValueCompressionModel extends ValueCompressionModel {
    private DataPoint initialDataPoint;
    private List<DataPoint> dataPoints;
    private boolean earlierAppendFailed;
    private LinearFunction upperBound;
    private LinearFunction lowerBound;
    private final Double errorBoundAsDecimal;

    public SwingValueCompressionModel(double errorBound) {
        super(errorBound);
        // The error bound provided is in percentage, so we first transform it to a decimal number
        // NOTE: due to the same reason as in ModelarDB we here choose to divide with 100.1 instead of 100
        // as we otherwise would allow data points slightly above the error bound.
        this.errorBoundAsDecimal = super.getErrorBound() / 100.1;
        this.resetModel();
    }

    @Override
    protected void resetModel() {
        initialDataPoint = null;
        dataPoints = new ArrayList<>();
        earlierAppendFailed = false;
    }

    @Override
    public int getLength() {
        return dataPoints.size();
    }

    @Override
    public boolean append(DataPoint dataPoint) {
        if (earlierAppendFailed) { // Security added so that if you try to append after an earlier append failed
            throw new IllegalArgumentException("You tried to append to the SWING-model after it failed an earlier append");
        }
        boolean withinErrorBound;
        double allowedDerivation = Math.abs(dataPoint.value() * errorBoundAsDecimal);

        if (this.getLength() < 2) {
            handleFirstTwoDataPoints(dataPoint, allowedDerivation);  // LINE 2-4: makes a recording and upper+lower bound
            withinErrorBound = true;
        } else {
            withinErrorBound = checkIfDataPointIsWithinErrorBound(dataPoint, allowedDerivation); // Line 12
            if (withinErrorBound) { // Line 23-27
                swingBounds(dataPoint, allowedDerivation);
            } else { // Line 13-22 (we ignore most of it as we only track one recording at a time)
                earlierAppendFailed = true;
            }
        }
        if (withinErrorBound) {
            dataPoints.add(dataPoint);
        }
        return withinErrorBound;
    }

    private void handleFirstTwoDataPoints(DataPoint dataPoint, double allowedDerivation) {
        if (initialDataPoint == null) { // First data point
            initialDataPoint = dataPoint;
        } else {
            lowerBound = new LinearFunction(initialDataPoint, new DataPoint(dataPoint.timestamp(), dataPoint.value() - allowedDerivation));
            upperBound = new LinearFunction(initialDataPoint, new DataPoint(dataPoint.timestamp(), dataPoint.value() + allowedDerivation));
        }
    }

    private boolean checkIfDataPointIsWithinErrorBound(DataPoint dataPoint, double allowedDerivation) {
        boolean withinErrorBound = true;
        if (dataPoint.value() < (lowerBound.getValue(dataPoint.timestamp()) - allowedDerivation)) {
            withinErrorBound = false;
        } else if (dataPoint.value() > (upperBound.getValue(dataPoint.timestamp()) + allowedDerivation)) {
            withinErrorBound = false;
        }
        return withinErrorBound;
    }

    private void swingBounds(DataPoint dataPoint, double allowedDerivation) {
        if (dataPoint.value() > (lowerBound.getValue(dataPoint.timestamp()) + allowedDerivation)) { // Swing up
            lowerBound = new LinearFunction(initialDataPoint, new DataPoint(dataPoint.timestamp(), dataPoint.value() - allowedDerivation));
        }
        if (dataPoint.value() < (upperBound.getValue(dataPoint.timestamp()) - allowedDerivation)) { // Swing down
            upperBound = new LinearFunction(initialDataPoint, new DataPoint(dataPoint.timestamp(), dataPoint.value() + allowedDerivation));
        }
    }

    @Override
    public ValueCompressionModelType getValueCompressionModelType() {
        return ValueCompressionModelType.SWING;
    }

    @Override
    public ByteBuffer getBlobRepresentation() {
        if (this.getLength() < 2) {
            throw new UnsupportedOperationException("Swing filter model needs at least two data points before you are able to get its value blob");
        }

        // We choose to save the average i.e. the line between our two bounds
        float slope = (float) ((this.lowerBound.getSlope() + this.upperBound.getSlope()) / 2);
        float intercept = (float) ((this.lowerBound.getIntercept() + this.upperBound.getIntercept()) / 2);

        // We convert to float as this is what we store (i.e. we support floating point precision)
        return ByteBuffer.allocate(8).putFloat(slope).putFloat(intercept);
    }

    @Override
    public void reduceToSizeN(int n) {
        // no implementation is necessary
    }
}
