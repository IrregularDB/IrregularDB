package segmentgenerator;

import compression.BaseModel;
import records.CompressionModel;
import compression.timestamp.TimestampCompressionModel;
import compression.value.ValueCompressionModel;

import java.util.Comparator;
import java.util.List;

public class ModelPicker{

    private ModelPicker(){
        //should not be instanciated
    }

    public static CompressionModel findBestCompressionModel(List<ValueCompressionModel> valueCompressionModels, List<TimestampCompressionModel> timestampCompressionModels){
        ValueCompressionModel bestValueCompressionModel = getBestValueModel(valueCompressionModels);
        TimestampCompressionModel bestTimestampCompressionModel = getBestTimeStampModel(timestampCompressionModels);

        return new CompressionModel(bestValueCompressionModel, bestTimestampCompressionModel);
    }

    private static double calculateAmountBytesPerDataPoint(BaseModel model) {
        // TODO: ensure this overhead is correct, maybe pass it as an input of some kind through the config
        // We have the following overhead:
        //   time_series_id (integer) = 4 bytes
        //   start_time (long/bigint) = 8 bytes
        //   end_time (int) = 4 bytes
        //   value_timestamp_model_type (smallint) = 2 bytes
        //   bytea (varbyte) in postgresql has an overhead of 4 bytes, this goes for both the blobs = 4 + 4 bytes
        int overhead = 4 + 8 + 4 + 2 + 4 + 4;
        int amountBytesUsed = overhead + model.getAmountBytesUsed();
        int amountDataPoints = model.getLength();
        return ((double) amountBytesUsed) / ((double) amountDataPoints);
    }

    private static ValueCompressionModel getBestValueModel(List<ValueCompressionModel> valueCompressionModels){
        return valueCompressionModels.stream()
                .min(Comparator.comparing(ModelPicker::calculateAmountBytesPerDataPoint))
                .orElseThrow(() -> new RuntimeException("In ModelPicker:getBestValueModel() - Should not happen"));
    }

    private static TimestampCompressionModel getBestTimeStampModel(List<TimestampCompressionModel> timestampCompressionModels) {
        return timestampCompressionModels.stream()
                .min(Comparator.comparing(ModelPicker::calculateAmountBytesPerDataPoint))
                .orElseThrow(() -> new RuntimeException("In ModelPicker:getBestTimeStampModel() - Should not happen"));    }
}
