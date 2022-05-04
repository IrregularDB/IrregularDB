package compression.value;

import compression.BaseModel;

public abstract class ValueCompressionModel extends BaseModel {

    private final Float errorBound;

    public ValueCompressionModel(Float errorBound, String cantConstructBlobErrorMessage, boolean adhereToLengthBound, int lengthBound) {
        super(cantConstructBlobErrorMessage, adhereToLengthBound, lengthBound);
        if (errorBound != null) {
            float temp = errorBound;
            if (temp == 0) {
                // HACK: Floating point imprecision can lead to error-bound of zero not really working.
                temp = 0.001F;
            }
            this.errorBound = temp / 100;
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
