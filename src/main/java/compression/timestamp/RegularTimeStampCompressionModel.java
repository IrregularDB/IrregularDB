package compression.timestamp;

import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class RegularTimeStampCompressionModel extends TimeStampCompressionModel {
    private int si;
    private boolean earlierAppendFailed;
    private List<Long> timeStamps;

    // TODO: update this constructor when adding error-bound
    public RegularTimeStampCompressionModel() {
        super(0);
        this.resetModel();
    }

    @Override
    protected void resetModel() {
        this.si = -1; // We use -1 to represent that no SI has been calculated yet.
        this.earlierAppendFailed = false;
        this.timeStamps = new ArrayList<>();
    }

    @Override
    public int getLength() {
        return timeStamps.size();
    }


    @Override
    public boolean append(DataPoint dataPoint) {
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
                withinErrorBound = isTimeStampWithinErrorBound(timeStamp);
                if (withinErrorBound) {
                    timeStamps.add(timeStamp);
                } else {
                    earlierAppendFailed = true;
                }
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
            si = calculateSI(timeStamps.get(timeStamps.size() - 1), timeStamp);
            timeStamps.add(timeStamp);
        }
    }


    private boolean isTimeStampWithinErrorBound(long timeStamp) {
        int actualSi = calculateSI(timeStamps.get(timeStamps.size() - 1), timeStamp);
        // TODO: add something where you use the actual error-bound for now we enforce error-bound = 0;
        return si == actualSi;
    }

    private int calculateSI(long previousTimeStamp, long timeStamp) {
        long si = timeStamp - previousTimeStamp;

        if (si < Integer.MIN_VALUE || si > Integer.MAX_VALUE) {
            throw new SiConversionException(si  + " the difference in timestamps cannot be cast to int without changing its value.");
        }
        return (int) si;
    }

    @Override
    public ByteBuffer getBlobRepresentation() {
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
    public void reduceToSizeN(int n) {
        //no implementation necessary
    }
}
