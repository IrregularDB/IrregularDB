package compression.timestamp;

import compression.encoding.BucketEncoding;
import compression.utility.LongToInt;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DeltaDeltaTimestampCompressionModel extends TimestampCompressionModel {
    private final static List<Integer> maxBucketValues = BucketEncoding.getMaxAbsoluteValuesOfResizeableBuckets();
    private List<Long> timestamps;
    private Long firstTimestamp;

    public DeltaDeltaTimestampCompressionModel(Integer threshold, int lengthBound) {
        super(threshold,
                "DeltaDelta time stamp model needs at least one data point before you are able to get the time stamp blob",
                true,
                lengthBound
        );
        // We make this a field so that we don't have to allocate a new signed bucket encoder each time get byte buffer is called
        resetModel();
    }

    @Override
    protected void resetModel() {
        this.timestamps = new ArrayList<>();
        this.firstTimestamp = null;
    }

    @Override
    public int getLength() {
        // The first time stamp is not included in the list
        return timestamps.size();
    }

    @Override
    protected boolean appendDataPoint(DataPoint dataPoint) {
        if (firstTimestamp == null) {
            firstTimestamp = dataPoint.timestamp();
        }

        long difference = dataPoint.timestamp() - firstTimestamp;
        if (difference < Integer.MAX_VALUE) {
            timestamps.add(dataPoint.timestamp());
            return true;
        } else {
            // Safety added, which should make it so that DeltaDelta does not try to store values bigger than INT_MAX
            return false;
        }
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        List<Integer> deltaDeltaTimestamps = new ArrayList<>();
        long firstTimestamp = timestamps.get(0);
        long secondTimestamp = timestamps.get(1);

        // Storing initial delta
        int previousDelta = calculateDelta(secondTimestamp, firstTimestamp);
        deltaDeltaTimestamps.add(previousDelta);
        long previousTimestamp = secondTimestamp;


        for (int i = 2; i < timestamps.size(); i++) {
            long currTimestamp = timestamps.get(i);

            int delta = calculateDelta(currTimestamp, previousTimestamp);
            int deltaOfDelta = LongToInt.calculateDifference(delta, previousDelta);

            long nextTimestamp = getNextTimestamp(i);
            int approximatedDeltaOfDelta = tryApplyThreshold(deltaOfDelta, previousDelta, previousTimestamp, nextTimestamp);

            // Update state and store delta-delta value
            int currDelta = previousDelta + approximatedDeltaOfDelta;
            previousDelta = currDelta;
            previousTimestamp += currDelta;
            deltaDeltaTimestamps.add(approximatedDeltaOfDelta);
        }

        return BucketEncoding.encode(deltaDeltaTimestamps, true);
    }

    private int calculateDelta(long currTimestamp, long previousTimestamp) {
        Integer delta = LongToInt.calculateDifference(currTimestamp, previousTimestamp);
        if (delta == null) {
            throw new IllegalStateException("Some how you got a delta that could not be represented by an integer. Should not be possible as we doing append data point check for this");
        }
        return delta;
    }

    private Long getNextTimestamp(int i) {
        long nextTimestamp;
        if (i < (timestamps.size() - 1)) {
            nextTimestamp = timestamps.get(i+1);
        } else {
            nextTimestamp = Long.MAX_VALUE; // last time stamp gets next value is LONG.MAX_VALUE
        }
        return nextTimestamp;
    }

    private Integer tryApplyThreshold(int deltaOfDelta, long previousDelta, long previousTimestamp, long nextTimestamp) {
        int absoluteValue = Math.abs(deltaOfDelta);
        for (Integer maxValue : maxBucketValues) {
            if (maxValue < absoluteValue && absoluteValue <= (maxValue + getThreshold())) {
                boolean isNegativeNumber = deltaOfDelta < 0;
                int pushedDeltaOfDelta = isNegativeNumber ? -maxValue : maxValue;
                long approximationOfCurrentTime = previousTimestamp + (previousDelta + pushedDeltaOfDelta);
                if (previousTimestamp < approximationOfCurrentTime && approximationOfCurrentTime < nextTimestamp) {
                    return pushedDeltaOfDelta; // We only use the pushed DeltaOfDelta if it creates timestamps between previous and current timestamp
                }
            }
        }
        return deltaOfDelta;
    }

    @Override
    public boolean canCreateByteBuffer() {
        return this.getLength() >= 2;
    }

    @Override
    protected void reduceToSize(int n) {
        this.timestamps.subList(n, timestamps.size()).clear();
    }

    @Override
    public TimestampCompressionModelType getTimestampCompressionModelType() {
        return TimestampCompressionModelType.DELTADELTA;
    }
}
