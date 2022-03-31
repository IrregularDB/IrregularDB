package compression.timestamp;

import compression.BaseModel;

public abstract class TimestampCompressionModel extends BaseModel {
    private final Integer errorBound;

    public TimestampCompressionModel(Integer threshold) {
        super();
        this.errorBound = threshold;
    }

    /**
     * Used to identify which type of model is used when writing to the DB
     * @return enum for the value compression model types
     */
    public abstract TimestampCompressionModelType getTimeStampCompressionModelType();

    public Integer getErrorBound() {
        if (errorBound == null) {
            throw new UnsupportedOperationException("You tried to get error bound for a model, which has no error bound defined");
        }
        return errorBound;
    }
}

