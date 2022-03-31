package compression.timestamp;

import compression.BaseModel;

public abstract class TimestampCompressionModel extends BaseModel {
    private final Integer threshold;

    public TimestampCompressionModel(Integer threshold) {
        super();
        this.threshold = threshold;
    }

    /**
     * Used to identify which type of model is used when writing to the DB
     * @return enum for the value compression model types
     */
    public abstract TimestampCompressionModelType getTimeStampCompressionModelType();

    public Integer getThreshold() {
        if (threshold == null) {
            throw new UnsupportedOperationException("You tried to get error bound for a model, which has no error bound defined");
        }
        return threshold;
    }
}

