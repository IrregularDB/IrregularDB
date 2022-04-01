package compression.value;

import compression.BaseModel;

public abstract class ValueCompressionModel extends BaseModel {

    private final Float errorBound;

    public ValueCompressionModel(Float errorBound) {
        super();

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
}
