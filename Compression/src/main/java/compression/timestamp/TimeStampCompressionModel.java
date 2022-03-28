package compression.timestamp;

import compression.BaseModel;
import compression.encoding.BucketEncoding;

public abstract class TimeStampCompressionModel extends BaseModel {

    protected BucketEncoding bucketEncoding = new BucketEncoding();

    public TimeStampCompressionModel(Float errorBound, Integer lengthBound) {
        super(errorBound, lengthBound);
    }

    /**
     * Used to identify which type of model is used when writing to the DB
     * @return enum for the value compression model types
     */
    public abstract TimeStampCompressionModelType getTimeStampCompressionModelType();
}

