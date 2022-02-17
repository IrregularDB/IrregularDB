package compression.timestamp;

import java.nio.ByteBuffer;

public class RegularTimeStampCompressionModel extends TimeStampCompressionModel {
    private int si;
    private boolean earlierAppendFailed;

    // TODO: update this constructor when adding error-bound
    public RegularTimeStampCompressionModel() {
        super(0);
    }

    @Override
    protected void resetModelParameters() {
        this.si = -1; // We use -1 to represent that no SI has been calculated yet.
        this.earlierAppendFailed = false;
    }

    @Override
    public boolean appendTimeStamp(long timeStamp) {
        try {
            if (earlierAppendFailed) { // Security added so that if you try to append after an earlier append failed
                throw new IllegalArgumentException("You tried to append to a model that had failed an earlier append");
            }

            boolean withinErrorBound;
            // Special handling for first two time stamps:
            withinErrorBound = isWithinErrorBound(timeStamp);

            if (withinErrorBound) {
                this.timeStamps.add(timeStamp);
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
        boolean withinErrorBound;
        if (this.si == -1) {
            withinErrorBound = handleFirstTwoDataPoints(timeStamp);
        } else {
            withinErrorBound = isTimeStampWithinErrorBound(timeStamp);
        }
        return withinErrorBound;
    }

    private boolean handleFirstTwoDataPoints(long timeStamp) {
        boolean withinErrorBound;
        if (this.getLength() == 0) {
            withinErrorBound = true;
        } else {
            this.si = calculateSI(timeStamp);
            withinErrorBound = true;
        }
        return withinErrorBound;
    }


    private boolean isTimeStampWithinErrorBound(long timeStamp) {
        int actualSi = calculateSI(timeStamp);
        // TODO: add something where you use the actual error-bound for now we enforce error-bound = 0;
        return this.si == actualSi;
    }

    private int calculateSI(long timeStamp) {
        long previousTimeStamp = timeStamps.get(this.getLength() - 1);

        long difference = timeStamp - previousTimeStamp;

        if (difference < Integer.MIN_VALUE || difference > Integer.MAX_VALUE) {
            throw new SiConversionException(difference  + " the difference in timestamps cannot be cast to int without changing its value.");
        }
        return (int) difference;
    }


    @Override
    public ByteBuffer getTimeStampBlob() {
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
        throw new RuntimeException("Not implemented");
    }
}
