package compression.timestamp;

import compression.BaseModel;
import compression.encoding.Encoding;

public abstract class TimeStampCompressionModel extends BaseModel {
    protected Encoding<Integer> encoding;

    public TimeStampCompressionModel(Float errorBound, Integer lengthBound, Encoding<Integer> encoding) {
        super(errorBound, lengthBound);
        this.encoding = encoding;
    }

    /**
     * Used to identify which type of model is used when writing to the DB
     * @return enum for the value compression model types
     */
    public abstract TimeStampCompressionModelType getTimeStampCompressionModelType();
}

