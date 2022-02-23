package segmentgenerator;

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

    private ValueCompressionModel getBestValueModel(){
        return valueCompressionModels.stream()
                .max(Comparator.comparing(ValueCompressionModel::getCompressionRatio))
                .orElseThrow(() -> new RuntimeException("In ModelPicker:getBestValueModel() - Should not happen"));
    }

    private TimeStampCompressionModel getBestTimeStampModel() {
        return this.timeStampCompressionModels.stream()
                .max(Comparator.comparing(TimeStampCompressionModel::getCompressionRatio))
                .orElseThrow(() -> new RuntimeException("In ModelPicker:getBestTimeStampModel() - Should not happen"));
    }
}
