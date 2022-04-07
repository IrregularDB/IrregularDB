package segmentgenerator;

import compression.BaseModel;
import records.CompressionModel;
import compression.timestamp.TimestampCompressionModel;
import compression.value.ValueCompressionModel;

import java.util.Comparator;
import java.util.List;

public abstract class ModelPicker{

    protected static final int overheadPerModel = calculateOverheadPerModel();

    public abstract CompressionModel findBestCompressionModel(List<ValueCompressionModel> valueCompressionModels, List<TimestampCompressionModel> timestampCompressionModels);

    protected double calculateAmountBytesPerDataPoint(int bytesUsedByModel, int modelLength) {
        int amountBytesUsed = overheadPerModel + bytesUsedByModel;
        int amountDataPoints = modelLength;
        return ((double) amountBytesUsed) / ((double) amountDataPoints);
    }

    protected double calculateAmountBytesPerDataPoint(BaseModel baseModel) {
        int amountBytesUsed = overheadPerModel + baseModel.getBlobRepresentation().position();
        int amountDataPoints = baseModel.getLength();
        return ((double) amountBytesUsed) / ((double) amountDataPoints);
    }

    private static int calculateOverheadPerModel() {
        // TODO: ensure this overhead is correct, maybe pass it as an input of some kind through the config
        // We have the following overhead:
        //   time_series_id (integer) = 4 bytes
        //   start_time (long/bigint) = 8 bytes
        //   end_time (int) = 4 bytes
        //   value_timestamp_model_type (smallint) = 2 bytes
        //   bytea (varbyte) in postgresql has an overhead of 4 bytes, this goes for both the blobs = 4 + 4 bytes
    int overhead = 4 + 8 + 4 + 2 + 4 + 4;
        return overhead/2; // there are two models to share the overhead
    }
}
