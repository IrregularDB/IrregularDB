package compression.timestamp;

import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
                withinErrorBound = isTimeStampWithinErrorBound(timeStamp, nextExpectedTimestamp, this.si, getErrorBound());
                if (withinErrorBound) {
                    timeStamps.add(timeStamp);
                } else {
                    Optional<Integer> newSI = attemptNewSI(timeStamp);
                    if (newSI.isPresent()) {
                        this.si = newSI.get();
                        withinErrorBound = true;
                    } else {
                        earlierAppendFailed = true;
                    }
                }
                this.nextExpectedTimestamp += si;
            }
            return withinErrorBound;
        } catch (SiConversionException e) {
            earlierAppendFailed = true;
            return false;
        }
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

    private Optional<Integer> attemptNewSI(long timestamp) {
        ArrayList<Long> allTimestamps = new ArrayList<>(this.timeStamps);
        allTimestamps.add(timestamp);

        ArrayList<Long> deltas = new ArrayList<>();
        for (int i = 1; i < allTimestamps.size(); i++) {
            deltas.add(allTimestamps.get(i) - allTimestamps.get(i -1));
        }

        OptionalDouble average = deltas.stream()
                .mapToLong(item -> item)
                .average();

        int candidateSI = (int) Math.round(average.getAsDouble());
        if (doesCandidateSIFit(allTimestamps, candidateSI)) {
            return Optional.of(candidateSI);
        } else {
            return Optional.empty();
        }
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


    private static boolean isTimeStampWithinErrorBound(long timeStamp, long nextExpectedTimestamp,int si, float errorBound) {
        int actualDifference = calculateDifference(timeStamp, nextExpectedTimestamp);
        double percentageError = actualDifference / ((double)si);
        // TODO: add something where you use the actual error-bound for now we enforce error-bound = 0;
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
        return ByteBuffer.allocate(4).putInt(si);
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
