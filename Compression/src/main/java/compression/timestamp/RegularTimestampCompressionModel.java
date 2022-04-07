package compression.timestamp;

import compression.encoding.SingleIntEncoding;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RegularTimestampCompressionModel extends TimestampCompressionModel {
    private int si;
    private boolean earlierAppendFailed;
    private List<Long> timestamps;
    private long nextExpectedTimestamp;

    public RegularTimestampCompressionModel(Integer threshold) {
        super(threshold, "Regular time stamp model needs at least two data points before you are able to get the time stamp blob");
        this.resetModel();
    }

    @Override
    protected void resetModel() {
        this.si = -1; // We use -1 to represent that no SI has been calculated yet.
        this.earlierAppendFailed = false;
        this.timestamps = new ArrayList<>();
        this.nextExpectedTimestamp = Long.MIN_VALUE;
    }

    @Override
    public int getLength() {
        return timestamps.size();
    }

    @Override
    protected boolean appendDataPoint(DataPoint dataPoint) {
        return appendTimestamp(dataPoint.timestamp());
    }

    private boolean appendTimestamp(long timeStamp) {
        if (earlierAppendFailed) { // Security added so that if you try to append after an earlier append failed
            throw new IllegalArgumentException("You tried to append to a model that had failed an earlier append");
        }
        boolean withinThreshold;

        // Special handling for first two time stamps:
        if (this.getLength() < 2) {
            handleFirstTwoDataPoints(timeStamp);
            withinThreshold = true;
        } else {
            withinThreshold = handleOtherDataPoints(timeStamp);

            this.nextExpectedTimestamp += si;
            if (!withinThreshold)
                earlierAppendFailed = true;
        }
        return withinThreshold;
    }

    private void handleFirstTwoDataPoints(long timestamp) {
        if (timestamps.size() == 0) {
            timestamps.add(timestamp);
        } else {
            si = calculateDifference(timestamps.get(timestamps.size() - 1), timestamp);
            timestamps.add(timestamp);
            nextExpectedTimestamp = timestamp + si;
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
        boolean fitNewSI = false;
        ArrayList<Long> allTimestamps = new ArrayList<>(this.timestamps);
        allTimestamps.add(timestamp);

        Integer candidateSI = calculateCandidateSI(allTimestamps);

        if (doesCandidateSIFit(allTimestamps, candidateSI)) {
            this.si = candidateSI;
            fitNewSI = true;
        }
        return fitNewSI;
    }

    private Integer calculateCandidateSI(List<Long> timestamps) {
        long duration = timestamps.get(timestamps.size() - 1) - timestamps.get(0);
        return Math.round((float)duration / (timestamps.size() - 1));
    }

    private boolean doesCandidateSIFit(ArrayList<Long> allTimestamps, int candidateSI) {
        long startTime = allTimestamps.get(0);
        long localNextExpectedTimestamp = startTime + candidateSI;

        for (int i = 1; i < allTimestamps.size(); i++) {
            boolean timestampWithinThreshold = isTimestampWithinThreshold(allTimestamps.get(i), localNextExpectedTimestamp, getThreshold());
            if (!timestampWithinThreshold) {
                return false;
            }
            localNextExpectedTimestamp += candidateSI;
        }
        return true;
    }

    private static boolean isTimestampWithinThreshold(long timestamp, long nextExpectedTimestamp, Integer threshold) {
        int actualDifference = calculateDifference(timestamp, nextExpectedTimestamp);
        return actualDifference <= threshold;
    }

    private static int calculateDifference(long timestamp1, long timestamp2) {
        return Math.abs(Math.toIntExact(timestamp2 - timestamp1));
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
