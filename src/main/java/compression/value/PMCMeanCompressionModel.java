// The implementation of this model type is based on code
// published in relation to ModelarDB
// LINK: https://github.com/skejserjensen/ModelarDB

package compression.value;

import compression.utility.PercentageError;

import java.nio.ByteBuffer;


public class PMCMeanCompressionModel extends ValueCompressionModel {
    private double min;
    private double max;
    private double sum;
    private boolean earlierAppendFailed;

    public PMCMeanCompressionModel(double errorBound) {
        super(errorBound);
    }

    private void updateModelState(double min, double max, double sum, double value) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.values.add(value);
    }

    @Override
    protected void resetModelParameters() {
        this.min = Double.POSITIVE_INFINITY;
        this.max = Double.NEGATIVE_INFINITY;
        this.sum = 0;
        this.earlierAppendFailed = false;
    }

    @Override
    public boolean appendValue(double value) {
        if (earlierAppendFailed) { // Security added so that if you try to append after an earlier append failed
            throw new IllegalArgumentException("You tried to append to a model that had failed an earlier append");
        }

        // We keep track of next values as we only update the min/max/sum if append succeeds
        double nextMin = Double.min(min, value);
        double nextMax = Double.max(max, value);
        double nextSum = sum + value;

        // Calculate average
        double mean = nextSum / (this.getLength() + 1);

        boolean appendSucceeded = PercentageError.isWithinErrorBound(mean, nextMin, errorBound) &&
                          PercentageError.isWithinErrorBound(mean, nextMax, errorBound);
        if (appendSucceeded) {
            updateModelState(nextMin, nextMax, nextSum, value);
        } else {
            this.earlierAppendFailed = true;
        }
        return appendSucceeded;
    }

    @Override
    public ByteBuffer getValueBlob() {
        if (this.getLength() == 0) {
            throw new UnsupportedOperationException("No data points where added to the model before trying to get the value blob");
        }

        // We convert to float as this is what we store
        float mean = (float) (this.sum / this.getLength());
        return ByteBuffer.allocate(4).putFloat(mean);
    }

    @Override
    public ValueCompressionModelType getValueCompressionModelType() {
        return ValueCompressionModelType.PMCMEAN;
    }

    @Override
    public void reduceToSizeN(int n) {
        throw new RuntimeException("Not implemented");
    }
}
