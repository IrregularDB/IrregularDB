package compression.timestamp;

import compression.BaseModel;
import records.DataPoint;

import java.util.List;
import java.util.function.Function;

public abstract class TimeStampCompressionModel extends BaseModel<Long> {
    public TimeStampCompressionModel(Double errorBound, Integer lengthBound) {
        super(errorBound, lengthBound);
    }

    /**
     * Used to identify which type of model is used when writing to the DB
     * @return enum for the value compression model types
     */
    public abstract TimeStampCompressionModelType getTimeStampCompressionModelType();
}

