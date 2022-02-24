package segmentgenerator;

import compression.BaseModel;
import compression.CompressionModel;
import compression.timestamp.TimeStampCompressionModel;
import compression.value.ValueCompressionModel;

import java.util.Comparator;
import java.util.List;

public record ModelPicker (List<ValueCompressionModel> valueCompressionModels, List<TimeStampCompressionModel> timeStampCompressionModels){

    public CompressionModel findBestCompressionModel(){
        ValueCompressionModel bestValueCompressionModel = getBestValueModel();
        TimeStampCompressionModel bestTimeStampCompressionModel = getBestTimeStampModel();

        return new CompressionModel(bestValueCompressionModel, bestTimeStampCompressionModel);
    }

    private <T> double calculateAmountBytesPerDataPoint(BaseModel model) {
        // TODO: ensure this overhead is correct, maybe pass it as an input of some kind through the config
        // We have the following overhead:
        //   time_series_id (integer) = 4 bytes
        //   start_time (long/bigint) = 8 bytes
        //   end_time (long/bigint) = 8 bytes
        //   model_type (smallint) = 2 bytes
        int overhead = 4 + 8 + 8 + 2;
        int amountBytesUsed = overhead + model.getAmountBytesUsed();
        int amountDataPoints = model.getLength();
        return ((double) amountBytesUsed) / ((double) amountDataPoints);
    }

    private ValueCompressionModel getBestValueModel(){
        return this.valueCompressionModels.stream()
                .min(Comparator.comparing(this::calculateAmountBytesPerDataPoint))
                .orElseThrow(() -> new RuntimeException("In ModelPicker:getBestValueModel() - Should not happen"));
    }

    private TimeStampCompressionModel getBestTimeStampModel() {
        return this.timeStampCompressionModels.stream()
                .min(Comparator.comparing(this::calculateAmountBytesPerDataPoint))
                .orElseThrow(() -> new RuntimeException("In ModelPicker:getBestTimeStampModel() - Should not happen"));    }
}
