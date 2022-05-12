package compression.timestamp;

import compression.encoding.SingleIntEncoding;
import compression.utility.LongToInt;
import records.DataPoint;
import records.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RegularTimestampCompressionModel extends TimestampCompressionModel {
    private Integer si;
    private boolean earlierAppendFailed;
    private List<Long> timestamps;
    private long nextExpectedTimestamp;
    private Long theNextTimestamp;

    public RegularTimestampCompressionModel(Integer threshold) {
        super(threshold,
                "Regular time stamp model needs at least two data points before you are able to get the time stamp blob",
                false,
                0
                );
        this.resetModel();
    }

    @Override
    protected void resetModel() {
        this.si = null; // We use null to represent that no SI has been calculated yet.
        this.earlierAppendFailed = false;
        this.timestamps = new ArrayList<>();
        this.nextExpectedTimestamp = Long.MIN_VALUE;
        this.theNextTimestamp = null;
    }

    @Override
    public int getLength() {
        return timestamps.size();
    }

    @Override
    protected boolean appendDataPoint(DataPoint dataPoint) {
        return appendTimestamp(dataPoint.timestamp());
    }

    private boolean appendTimestamp(long timestamp) {
        if (earlierAppendFailed) { // Security added so that if you try to append after an earlier append failed
            throw new IllegalArgumentException("You tried to append to a model that had failed an earlier append");
        }
        boolean withinThreshold;

        // Special handling for first two time stamps:
        if (this.getLength() < 2) {
            withinThreshold = handleFirstTwoDataPoints(timestamp);
            if (si != null) {
                nextExpectedTimestamp = timestamp + si;
            }
        } else {
            withinThreshold = handleOtherDataPoints(timestamp);
            this.nextExpectedTimestamp += si;
        }
        if (!withinThreshold)
            earlierAppendFailed = true;
        return withinThreshold;
    }

    private boolean handleFirstTwoDataPoints(long timestamp) {
        if (timestamps.size() == 0) {
            timestamps.add(timestamp);
            return true;
        } else {
            si = LongToInt.calculateDifference(timestamp, timestamps.get(0));
            if (si == null) { // The SI could not be fitted into an integer.
                return false;
            } else {
                timestamps.add(timestamp);
                return true;
            }
        }
    }

    private boolean handleOtherDataPoints(long timestamp) {
        boolean withinThreshold = isTimestampWithinThreshold(timestamp, nextExpectedTimestamp, getThreshold());
        if (withinThreshold) {
            timestamps.add(timestamp);
        } else {
            withinThreshold = testNewCandidateSI(timestamp);
            if (withinThreshold) {
                timestamps.add(timestamp);
            }
        }
        return withinThreshold;
    }

    private boolean testNewCandidateSI(long timestamp) {
        ArrayList<Long> allTimestamps = new ArrayList<>(this.timestamps);
        allTimestamps.add(timestamp);

        Integer candidateSI = calculateCandidateSI(allTimestamps);
        Pair<Boolean, Long> candidateSIResult = doesCandidateSIFit(allTimestamps, candidateSI);
        if (candidateSIResult.f0()) {
            this.si = candidateSI;
            this.nextExpectedTimestamp = candidateSIResult.f1();
            return true;
        }
        return false;
    }

    private Integer calculateCandidateSI(List<Long> timestamps) {
        long duration = timestamps.get(timestamps.size() - 1) - timestamps.get(0);
        return Math.round((float)duration / (timestamps.size() - 1));
    }

    private Pair<Boolean, Long> doesCandidateSIFit(ArrayList<Long> allTimestamps, int candidateSI) {
        long localNextExpectedTimestamp = allTimestamps.get(0);
        Integer threshold = getThreshold();

        for (int i = 1; i < allTimestamps.size(); i++) {
            localNextExpectedTimestamp += candidateSI;
            boolean timestampWithinThreshold = isTimestampWithinThreshold(allTimestamps.get(i), localNextExpectedTimestamp,threshold);
            if (!timestampWithinThreshold) {
                return new Pair<>(false, null);
            }
        }
        return new Pair<>(true, localNextExpectedTimestamp);
    }

    private boolean isTimestampWithinThreshold(long timestamp, long nextExpectedTimestamp, Integer threshold) {
        Integer difference = LongToInt.calculateDifference(nextExpectedTimestamp, timestamp);
        if (difference == null) {
            return false;
        } else {
            return Math.abs(difference) <= threshold;
        }
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        return SingleIntEncoding.encode(si);
    }

    @Override
    public boolean canCreateByteBuffer() {
        return getLength() >= 2;
    }

    @Override
    public TimestampCompressionModelType getTimestampCompressionModelType() {
        return TimestampCompressionModelType.REGULAR;
    }

    @Override
    protected void reduceToSize(int n) {
        timestamps.subList(n, this.getLength()).clear();
    }
}
