package compression.timestamp;

import compression.encoding.SignedBucketEncoder;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SIDiffTimestampCompressionModel extends TimestampCompressionModel {
    private final SignedBucketEncoder signedBucketEncoder;
    private List<Long> timestamps;

    public SIDiffTimestampCompressionModel(Integer errorBound) {
        super(errorBound);
        // We make this a field so that we don't have to allocate a new signed bucket encoder each time get byte buffer is called
        signedBucketEncoder = new SignedBucketEncoder();
        this.resetModel();
    }

    @Override
    protected void resetModel() {
        this.timestamps = new ArrayList<>();
    }

    @Override
    public int getLength() {
        return timestamps.size();
    }

    @Override
    protected boolean appendDataPoint(DataPoint dataPoint) {
        timestamps.add(dataPoint.timestamp());
        return true;
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        List<Integer> readings = new ArrayList<>();
        int si = calculateSI();
        readings.add(si);

        long firstTimestamp = timestamps.get(0);
        int allowedDerivation = (int)(si * getErrorBound());//TODO replace with threshold
        List<Integer> maxValuesOfBuckets = signedBucketEncoder.getMaxAbsoluteValuesOfResizeableBuckets();

        long approximation = firstTimestamp + (long) si;
        // We skip the first timestamp as it is stored on the segment
        for (int i = 1; i < timestamps.size(); i++) {
            int difference = calculateDifference(timestamps.get(i), approximation, maxValuesOfBuckets, allowedDerivation);
            readings.add(difference);
            approximation += si;
        }
        return signedBucketEncoder.encode(readings).getFinishedByteBuffer();
    }

    private int calculateSI() {
        long firstTimestamp = timestamps.get(0);
        long lastTimestamp = timestamps.get(timestamps.size() - 1);

        long duration = lastTimestamp - firstTimestamp;
        return Math.round((float) duration / (timestamps.size() - 1));
    }

    private int calculateDifference(long currentTimestamp, long approximation, List<Integer> maxValuesOfBuckets, int allowedDerivation) {
        int difference = Math.toIntExact(currentTimestamp - approximation);
        int absoluteDifference = Math.abs(difference);

        // We look for values that are between max value and max value + allowed derivation
        for (int maxValue : maxValuesOfBuckets) {
            if (maxValue <= absoluteDifference && absoluteDifference <= (maxValue + allowedDerivation)) {
                boolean isNegativeNumber = difference < 0;
                difference = isNegativeNumber ? -1 * maxValue : maxValue;
                break;
            }
        }
        return difference;
    }

    @Override
    protected void reduceToSize(int n) {
        this.timestamps.subList(n, timestamps.size()).clear();
    }

    @Override
    public TimestampCompressionModelType getTimeStampCompressionModelType() {
        return TimestampCompressionModelType.SIDIFF;
    }
}
