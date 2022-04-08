package compression.timestamp;

import compression.encoding.SignedBucketEncoder;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DeltaDeltaTimestampCompressionModel extends TimestampCompressionModel {
    private final SignedBucketEncoder signedBucketEncoder;
    private final List<Integer> maxBucketValues;
    private List<Integer> deltaDeltaTimestamps;
    private Long previousTimestamp = null;
    private Integer previousDelta;

    public DeltaDeltaTimestampCompressionModel(Integer threshold) {
        super(threshold, "DeltaDelta time stamp model needs at least one data point before you are able to get the time stamp blob");
        // We make this a field so that we don't have to allocate a new signed bucket encoder each time get byte buffer is called
        signedBucketEncoder = new SignedBucketEncoder();
        this.maxBucketValues = signedBucketEncoder.getMaxAbsoluteValuesOfResizeableBuckets();

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
            int delta = calculateDelta(dataPoint);
            int approximatedDelta = tryApplyThreshold(delta);

            previousDelta = approximatedDelta;
            previousTimestamp = previousTimestamp + previousDelta;
            deltaDeltaTimestamps.add(approximatedDelta);
        } else {
            int deltaOfDelta = calculateDelta(dataPoint) - previousDelta;
            int approximatedDeltaOfDelta = tryApplyThreshold(deltaOfDelta);

            previousDelta = previousDelta + approximatedDeltaOfDelta;
            previousTimestamp = previousTimestamp + previousDelta;
            deltaDeltaTimestamps.add(approximatedDeltaOfDelta);
        }
        return true;
    }

    private int calculateDelta(DataPoint dataPoint) {
        return (int) (dataPoint.timestamp() - previousTimestamp);
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
        return signedBucketEncoder.encode(this.deltaDeltaTimestamps).getFinishedByteBuffer();
    }

    @Override
    public boolean canCreateByteBuffer() {
        return previousTimestamp != null;
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
