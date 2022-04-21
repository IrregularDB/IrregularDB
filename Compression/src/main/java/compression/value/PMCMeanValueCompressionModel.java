// The implementation of this model type is based on code
// published in relation to ModelarDB
// LINK: https://github.com/skejserjensen/ModelarDB

package compression.value;

import compression.utility.PercentageError;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class PMCMeanValueCompressionModel extends ValueCompressionModel {
    private float min;
    private float max;
    private float sum;
    private boolean earlierAppendFailed;
    private int length;

    public PMCMeanValueCompressionModel(float errorBound) {
        super(errorBound,
                "No data points where added to the PMC-mean value model before trying to get the value blob",
                false,
                0
                );
        this.resetModel();
    }

    @Override
    protected void resetModel() {
        this.min = Float.POSITIVE_INFINITY;
        this.max = Float.NEGATIVE_INFINITY;
        this.sum = 0;
        this.earlierAppendFailed = false;
        this.length = 0;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    protected boolean appendDataPoint(DataPoint dataPoint) {
        return appendValue(dataPoint.value());
    }

    private boolean appendValue(float value) {
        if (earlierAppendFailed) { // Security added so that if you try to append after an earlier append failed
            throw new IllegalArgumentException("You tried to append to a model that had failed an earlier append");
        }

        // We keep track of next values as we only update the min/max/sum if append succeeds
        float nextMin = Float.min(min, value);
        float nextMax = Float.max(max, value);
        float nextSum = sum + value;

        // Calculate average
        float mean = nextSum / (this.getLength() + 1);

        boolean appendSucceeded = PercentageError.isWithinErrorBound(mean, nextMin, super.getErrorBound()) &&
                          PercentageError.isWithinErrorBound(mean, nextMax, super.getErrorBound());
        if (appendSucceeded) {
            updateModelState(nextMin, nextMax, nextSum);
        } else {
            this.earlierAppendFailed = true;
        }
        return appendSucceeded;
    }

    private void updateModelState(float min, float max, float sum) {
        this.min = min;
        this.max = max;
        this.sum = sum;
        this.length++;
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        // We convert to float as this is what we store (i.e. we support floating point precision)
        float mean = (this.sum / this.getLength());
        return ByteBuffer.allocate(4).putFloat(mean);
    }

    @Override
    public boolean canCreateByteBuffer() {
        return getLength() != 0;
    }

    @Override
    public ValueCompressionModelType getValueCompressionModelType() {
        return ValueCompressionModelType.PMC_MEAN;
    }

    @Override
    protected void reduceToSize(int n) {
        // Do nothing except update length (if the constant model could fit the longer list of values it can also fit the shorter list)
        length = n;
    }
}
