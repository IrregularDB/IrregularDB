package compression.timestamp;

import compression.BaseModel;

public abstract class TimeStampCompressionModel extends BaseModel {

    public TimeStampCompressionModel(Float errorBound, Integer lengthBound) {
        super(errorBound, lengthBound);
    }

    /**
     * Used to identify which type of model is used when writing to the DB
     * @return enum for the value compression model types
     */
    public abstract TimeStampCompressionModelType getTimeStampCompressionModelType();
}

