package compression.timestamp;

import compression.BaseModel;
import compression.encoding.BucketEncoding;

public abstract class TimeStampCompressionModel extends BaseModel {

    protected BucketEncoding bucketEncoding = new BucketEncoding();
    private final Integer errorBound;

    public TimeStampCompressionModel(Integer errorBound, Integer lengthBound) {
        super(lengthBound);
        this.errorBound = errorBound;
    }

    /**
     * Used to identify which type of model is used when writing to the DB
     * @return enum for the value compression model types
     */
    public abstract TimeStampCompressionModelType getTimeStampCompressionModelType();

    public Integer getErrorBound() {
        if (errorBound == null) {
            throw new UnsupportedOperationException("You tried to get error bound for a model, which has no error bound defined");
        }
        return errorBound;
    }
}

