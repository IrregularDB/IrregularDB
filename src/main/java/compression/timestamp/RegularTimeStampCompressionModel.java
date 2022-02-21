package compression.timestamp;

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
    protected boolean appendTimeStamp(long timeStamp) {
        try {
            if (earlierAppendFailed) { // Security added so that if you try to append after an earlier append failed
                throw new IllegalArgumentException("You tried to append to a model that had failed an earlier append");
            }
            boolean withinErrorBound;
            // Special handling for first two time stamps:
            withinErrorBound = isWithinErrorBound(timeStamp);

            if (withinErrorBound) {
                timeStamps.add(timeStamp);
            } else {
                earlierAppendFailed = true;
            }
            return withinErrorBound;
        } catch (SiConversionException e) {
            earlierAppendFailed = true;
            return false;
        }
    }

    private boolean isWithinErrorBound(long timeStamp) {
        boolean withinErrorBound = true;
        if (this.si == -1) {
            handleFirstTwoDataPoints(timeStamp);
        } else {
            withinErrorBound = isTimeStampWithinErrorBound(timeStamp);
        }
        return withinErrorBound;
    }

    private void handleFirstTwoDataPoints(long timeStamp) {
        if (!timeStamps.isEmpty()) {
            si = calculateSI(timeStamp);
        }
    }

    private boolean isTimeStampWithinErrorBound(long timeStamp) {
        int actualSi = calculateSI(timeStamp);
        // TODO: add something where you use the actual error-bound for now we enforce error-bound = 0;
        return si == actualSi;
    }

    private int calculateSI(long timeStamp) {
        long previousTimeStamp = timeStamps.get(timeStamps.size() - 1);

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
