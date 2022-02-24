package compression.value;

import compression.BaseModel;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
