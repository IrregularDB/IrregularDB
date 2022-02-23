package compression.value;

import compression.BaseModel;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class ValueCompressionModel extends BaseModel<Double> {
    public ValueCompressionModel(double errorBound) {
        super(errorBound);
    }

    /**
     * Used to identify which type of model is used when writing to the DB
     * @return enum for the value compression model types
     */
    public abstract ValueCompressionModelType getValueCompressionModelType();

}
