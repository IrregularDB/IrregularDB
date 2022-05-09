package compression.timestamp;

import compression.encoding.BucketEncoding;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SIDiffTimestampCompressionModel extends TimestampCompressionModel {
    private Long firstTimestamp;
    private List<Long> timestamps;

    public SIDiffTimestampCompressionModel(Integer threshold, int lengthBound) {
        super(threshold,
                "SIdiff time stamp model needs at least two data points before you are able to get the time stamp blob",
                true,
                lengthBound
                );
        // We make this a field so that we don't have to allocate a new signed bucket encoder each time get byte buffer is called
        this.resetModel();
    }

    @Override
    protected void resetModel() {
        this.timestamps = new ArrayList<>();
        this.firstTimestamp = null;
    }

    @Override
    public int getLength() {
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
            // Safety added, which should make it so that SI-diff does not try to store values bigger than INT_MAX
            return false;
        }
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        List<Integer> readings = new ArrayList<>();
        int si = calculateSI();
        readings.add(si);

        int allowedDerivation = getThreshold();
        List<Integer> maxValuesOfBuckets = BucketEncoding.getMaxAbsoluteValuesOfResizeableBuckets();

        long approximation = firstTimestamp + (long) si;
        long previousTimestamp = firstTimestamp;
        // We skip the first timestamp as it is stored on the segment
        for (int i = 1; i < timestamps.size(); i++) {
            Long nextTimestamp = getNextTimestamp(i);
            int difference = calculateDifference(timestamps.get(i), nextTimestamp, previousTimestamp, approximation, maxValuesOfBuckets, allowedDerivation);
            readings.add(difference);
            approximation += si;
            previousTimestamp = approximation;
        }

        return BucketEncoding.encode(readings, true);
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

    @Override
    public boolean canCreateByteBuffer() {
        return getLength() >= 2;
    }


    private int calculateSI() {
        long firstTimestamp = timestamps.get(0);
        long lastTimestamp = timestamps.get(timestamps.size() - 1);

        long duration = lastTimestamp - firstTimestamp;
        return Math.round((float) duration / (timestamps.size() - 1));
    }

    private int calculateDifference(long currentTimestamp, long nextTimestamp, long previousTimestamp, long approximation, List<Integer> maxValuesOfBuckets, int allowedDerivation) {
        int difference = Math.toIntExact(currentTimestamp - approximation);
        int absoluteDifference = Math.abs(difference);

        // We look for values that are between max value and max value + allowed derivation
        for (int maxValue : maxValuesOfBuckets) {
            if (maxValue <= absoluteDifference && absoluteDifference <= (maxValue + allowedDerivation)) {
                boolean isNegativeNumber = difference < 0;
                int pushedDifference = isNegativeNumber ? -1 * maxValue : maxValue;
                long pushedApproximation = approximation + pushedDifference;
                if (previousTimestamp < pushedApproximation && pushedApproximation < nextTimestamp) {
                    return pushedDifference; // We only use the pushed difference if it is between the previous and next time stamp
                }
            }
        }
        return difference;
    }

    @Override
    protected void reduceToSize(int n) {
        this.timestamps.subList(n, timestamps.size()).clear();
    }

    @Override
    public TimestampCompressionModelType getTimestampCompressionModelType() {
        return TimestampCompressionModelType.SIDIFF;
    }
}
