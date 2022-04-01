package compression.value;

import compression.BaseModel;

public abstract class ValueCompressionModel extends BaseModel {

    private final Float errorBound;
    private final Integer lengthBound;

    public ValueCompressionModel(Float errorBound, Integer lengthBound) {
        super();

        this.lengthBound = lengthBound;

        // HACK: Floating point imprecision can lead to error-bound of zero not really working.
        if (errorBound != null) {
            if (errorBound == 0) {
                this.errorBound = 0.00001F;
            } else {
                this.errorBound = errorBound / 100;
            }
        } else {
            this.errorBound = null;
        }
    }

    /**
     * Used to identify which type of model is used when writing to the DB
     * @return enum for the value compression model types
     */
    public abstract ValueCompressionModelType getValueCompressionModelType();

    public float getErrorBound() {
        if (errorBound == null) {
            throw new UnsupportedOperationException("You tried to get error bound for a model, which has no error bound defined");
        }
        return errorBound;
    }

    public int getLengthBound() {
        if (lengthBound == null) {
            throw new UnsupportedOperationException("You tried to get length bound for a model, which has no length bound defined");
        }
        return lengthBound;
    }
}
