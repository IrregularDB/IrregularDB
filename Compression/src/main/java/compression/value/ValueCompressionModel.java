package compression.value;

import compression.BaseModel;

public abstract class ValueCompressionModel extends BaseModel {
    public ValueCompressionModel(Float errorBound, Integer lengthBound) {
        super(errorBound, lengthBound);
    }

    /**
     * Used to identify which type of model is used when writing to the DB
     * @return enum for the value compression model types
     */
    public abstract ValueCompressionModelType getValueCompressionModelType();

}
