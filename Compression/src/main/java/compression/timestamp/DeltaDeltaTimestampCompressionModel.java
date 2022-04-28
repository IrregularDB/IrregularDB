package compression.timestamp;

import compression.encoding.BucketEncoding;
import compression.utility.LongToInt;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DeltaDeltaTimestampCompressionModel extends TimestampCompressionModel {
    private final List<Integer> maxBucketValues;
    private List<Integer> deltaDeltaTimestamps;
    private Long previousTimestamp = null;
    private Integer previousDelta;

    public DeltaDeltaTimestampCompressionModel(Integer threshold, int lengthBound) {
        super(threshold,
                "DeltaDelta time stamp model needs at least one data point before you are able to get the time stamp blob",
                true,
                lengthBound
                );
        // We make this a field so that we don't have to allocate a new signed bucket encoder each time get byte buffer is called
        this.maxBucketValues = BucketEncoding.getMaxAbsoluteValuesOfResizeableBuckets();
        resetModel();
    }

    @Override
    protected void resetModel() {
        this.deltaDeltaTimestamps = new ArrayList<>();
        this.previousTimestamp = null;
        this.previousDelta = null;
    }

    @Override
    public int getLength() {
        // The first time stamp is not included in the list
        return 1 + this.deltaDeltaTimestamps.size();
    }

    @Override
    protected boolean appendDataPoint(DataPoint dataPoint) {
        if (this.deltaDeltaTimestamps.size() == 0 && previousTimestamp == null){
            // Don't store anything for first timestamp but remember it for next time
            previousTimestamp = dataPoint.timestamp();
        } else if (this.deltaDeltaTimestamps.size() == 0 && previousDelta == null) {
            // Handle second data point by storing its delta
            long delta = calculateDelta(dataPoint.timestamp(), previousTimestamp);
            Integer intRepresentationOfDelta = LongToInt.castToInt(delta);
            if (intRepresentationOfDelta == null) { // Delta was too large.
                return false;
            }
            int approximatedDelta = tryApplyThreshold(intRepresentationOfDelta);
            previousDelta = approximatedDelta;
            previousTimestamp = previousTimestamp + previousDelta;
            deltaDeltaTimestamps.add(approximatedDelta);
        } else {
            long delta = calculateDelta(dataPoint.timestamp(), previousTimestamp);
            Integer deltaOfDelta = LongToInt.calculateDifference(delta, previousDelta);
            if (deltaOfDelta == null) { // Delta-of-delta was too large.
                return false;
            }
            int approximatedDeltaOfDelta = tryApplyThreshold(deltaOfDelta);
            previousDelta = previousDelta + approximatedDeltaOfDelta;
            previousTimestamp = previousTimestamp + previousDelta;
            deltaDeltaTimestamps.add(approximatedDeltaOfDelta);
        }
        return true;
    }

    private long calculateDelta(long currTimestamp, long previousTimestamp) {
        return currTimestamp - previousTimestamp;
    }

    private Integer tryApplyThreshold(int value) {
        int absoluteValue = Math.abs(value);
        for (Integer maxValue : maxBucketValues) {
            if (maxValue < absoluteValue && (absoluteValue - getThreshold()) <= maxValue) {
               return value < 0 ? -maxValue : maxValue;
            }
        }
        return value;
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        return BucketEncoding.encode(this.deltaDeltaTimestamps, true);
    }

    @Override
    public boolean canCreateByteBuffer() {
        return deltaDeltaTimestamps.size() != 0;
    }

    @Override
    protected void reduceToSize(int n) {
        // We have to cut the list down to size n-1 as the first time stamp is not in the list
        this.deltaDeltaTimestamps.subList(n - 1, deltaDeltaTimestamps.size()).clear();
    }

    @Override
    public TimestampCompressionModelType getTimestampCompressionModelType() {
        return TimestampCompressionModelType.DELTADELTA;
    }
}
