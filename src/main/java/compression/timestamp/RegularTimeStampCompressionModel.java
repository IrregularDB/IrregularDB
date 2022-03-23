package compression.timestamp;

import compression.encoding.SingleIntEncoding;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;

public class RegularTimeStampCompressionModel extends TimeStampCompressionModel {
    private int si;
    private boolean earlierAppendFailed;
    private List<Long> timeStamps;
    private long nextExpectedTimestamp;

    // TODO: update this constructor when adding error-bound
    public RegularTimeStampCompressionModel(float errorBound) {
        super(errorBound, null);
        this.resetModel();
    }

    @Override
    protected void resetModel() {
        this.si = -1; // We use -1 to represent that no SI has been calculated yet.
        this.earlierAppendFailed = false;
        this.timeStamps = new ArrayList<>();
        this.nextExpectedTimestamp = Long.MIN_VALUE;
    }

    @Override
    public int getLength() {
        return timeStamps.size();
    }

    @Override
    protected boolean appendDataPoint(DataPoint dataPoint) {
        return appendTimeStamp(dataPoint.timestamp());
    }

    private boolean appendTimeStamp(long timeStamp) {
        try {
            if (earlierAppendFailed) { // Security added so that if you try to append after an earlier append failed
                throw new IllegalArgumentException("You tried to append to a model that had failed an earlier append");
            }
            boolean withinErrorBound;

            // Special handling for first two time stamps:
            if (this.getLength() < 2) {
                handleFirstTwoDataPoints(timeStamp);
                withinErrorBound = true;
            } else {
                withinErrorBound = handleOtherDataPoints(timeStamp);
                if (withinErrorBound)
                    this.nextExpectedTimestamp += si;
            }
            return withinErrorBound;
        } catch (SiConversionException e) {
            earlierAppendFailed = true;
            return false;
        }
    }

    private boolean handleOtherDataPoints(long timeStamp) {
        boolean withinErrorBound = isTimeStampWithinErrorBound(timeStamp, nextExpectedTimestamp, this.si, getErrorBound());
        if (withinErrorBound) {
            timeStamps.add(timeStamp);
        } else {
            withinErrorBound = handleDataPointNotWithinErrorBound(timeStamp);
        }
        return withinErrorBound;
    }

    private boolean handleDataPointNotWithinErrorBound(long timestamp) {
        boolean fitNewSI = false;
        ArrayList<Long> allTimestamps = new ArrayList<>(this.timeStamps);
        allTimestamps.add(timestamp);

        Integer candidateSI = calculateCandidateSI(allTimestamps);

        if (doesCandidateSIFit(allTimestamps, candidateSI)) {
            this.si = candidateSI;
            fitNewSI = true;
            this.timeStamps.add(timestamp);
        } else {
            earlierAppendFailed = true;
        }
        return fitNewSI;
    }

    private void handleFirstTwoDataPoints(long timeStamp) {
        if (timeStamps.size() == 0) {
            timeStamps.add(timeStamp);
        } else {
            si = calculateDifference(timeStamps.get(timeStamps.size() - 1), timeStamp);
            timeStamps.add(timeStamp);
            nextExpectedTimestamp = timeStamp + si;
        }
    }

    private Integer calculateCandidateSI(List<Long> timestamps) {
        long duration = timestamps.get(timestamps.size() - 1) - timestamps.get(0);
        return Math.round((float)duration / (timestamps.size() - 1));
    }

    private boolean doesCandidateSIFit(ArrayList<Long> allTimestamps, int candidateSI) {
        long startTime = allTimestamps.get(0);
        long localNextExpectedTimestamp = startTime + candidateSI;

        for (int i = 1; i < allTimestamps.size(); i++) {
            boolean timeStampWithinErrorBound = isTimeStampWithinErrorBound(allTimestamps.get(i), localNextExpectedTimestamp, candidateSI, getErrorBound());
            if (!timeStampWithinErrorBound) {
                return false;
            }
        }
        return true;
    }

    private static boolean isTimeStampWithinErrorBound(long timeStamp, long nextExpectedTimestamp, int si, float errorBound) {
        int actualDifference = calculateDifference(timeStamp, nextExpectedTimestamp);
        double percentageError = actualDifference / ((double)si);
        return percentageError <= errorBound;
    }

    private static int calculateDifference(long timestamp1, long timeStamp2) {
        long difference = Math.abs(timeStamp2 - timestamp1);

        if (difference < Integer.MIN_VALUE || difference > Integer.MAX_VALUE) {
            throw new SiConversionException(difference  + " the difference in timestamps cannot be cast to int without changing its value.");
        }
        return (int) difference;
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        if (this.getLength() < 2) {
            throw new UnsupportedOperationException("Regular time stamp model needs at least two data points before you are able to get the time stamp blob");
        }
        return SingleIntEncoding.encode(si);
    }

    @Override
    public TimeStampCompressionModelType getTimeStampCompressionModelType() {
        return TimeStampCompressionModelType.REGULAR;
    }

    @Override
    protected void reduceToSize(int n) {
        int length = this.getLength();
        if (length < n) {
            throw new IllegalArgumentException("You tried to reduce this size of a model to something smaller than its current size");
        }
        timeStamps.subList(n, this.getLength()).clear();
    }
}
