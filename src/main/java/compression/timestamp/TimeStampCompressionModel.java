package compression.timestamp;

import compression.BaseModel;

import java.util.List;

public abstract class TimeStampCompressionModel extends BaseModel<Long> {
    public TimeStampCompressionModel(double errorBound) {
        super(errorBound);
    }

    /**
     * Used to identify which type of model is used when writing to the DB
     * @return enum for the value compression model types
     */
    public abstract TimeStampCompressionModelType getTimeStampCompressionModelType();

    @Override
    public boolean append(Long timeStamp) {
        // Done in order to ensure that we don't get any reference problems by appending simple types instead
        return appendTimeStamp(timeStamp);
    }

    protected abstract boolean appendTimeStamp(long timeStamp);
}

