package compression.timestamp;

import compression.BaseModel;
import records.DataPoint;

import java.util.List;
import java.util.function.Function;

public abstract class TimeStampCompressionModel extends BaseModel<Long> {
    public TimeStampCompressionModel(double errorBound) {
        super(errorBound);
    }

    /**
     * Used to identify which type of model is used when writing to the DB
     * @return enum for the value compression model types
     */
    public abstract TimeStampCompressionModelType getTimeStampCompressionModelType();

    @Override
    public boolean append(Long timeStamp) {
        // Done in order to ensure that we don't get any reference problems by appending simple types instead
        return appendTimeStamp(timeStamp);
    }

    public boolean resetAndAppendAll(List<DataPoint> input){
        return resetAndAppendAll(input, DataPoint::timestamp);
    }


    protected abstract boolean appendTimeStamp(long timeStamp);
}

