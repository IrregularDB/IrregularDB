package segmentgenerator;

import records.CompressionModel;
import compression.timestamp.TimestampCompressionModel;
import compression.value.ValueCompressionModel;
import records.DataPoint;
import records.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompressionModelManager {
    private final ModelPicker modelPicker;

    private final List<Pair<ValueCompressionModel, Boolean>> valueModelPairs;
    private final List<Pair<TimestampCompressionModel, Boolean>> timestampModelPairs;

    public CompressionModelManager(List<ValueCompressionModel> valueCompressionModels, List<TimestampCompressionModel> timestampCompressionModels,
                                  ModelPicker modelPicker) {
        this.valueModelPairs = new ArrayList<>();
        this.timestampModelPairs = new ArrayList<>();
        for (ValueCompressionModel valueModel : valueCompressionModels) {
            this.valueModelPairs.add(new Pair<>(valueModel, true));
        }
        for (TimestampCompressionModel timestampModel : timestampCompressionModels) {
            this.timestampModelPairs.add(new Pair<>(timestampModel, true));
        }
        this.modelPicker = modelPicker;
    }

    public boolean tryAppendDataPointToAllModels(DataPoint dataPoint) {
        // Partition models by append success
        boolean anyValueModelsStillActive = false;
        for (Pair<ValueCompressionModel, Boolean> valueModelPair : valueModelPairs) {
            if (valueModelPair.getF1()) { // Only append to the still active models
                boolean appendSuccessful = valueModelPair.getF0().append(dataPoint);
                valueModelPair.setF1(appendSuccessful);
                anyValueModelsStillActive = anyValueModelsStillActive | appendSuccessful;
            }
        }

        boolean anyTimeStampModelsStillActive = false;
        for (Pair<TimestampCompressionModel, Boolean> timestampModelPair : timestampModelPairs) {
            if (timestampModelPair.getF1()) { // Only append to the still active models
                boolean appendSuccessful = timestampModelPair.getF0().append(dataPoint);
                timestampModelPair.setF1(appendSuccessful);
                anyTimeStampModelsStillActive = anyTimeStampModelsStillActive | appendSuccessful;
            }
        }

        return anyValueModelsStillActive && anyTimeStampModelsStillActive;
    }

    public boolean resetAndTryAppendBuffer(List<DataPoint> notYetEmitted) {
        boolean anyValueModelsStillActive = false;
        for (Pair<ValueCompressionModel, Boolean> valueModelPair : valueModelPairs) {
            boolean appendSuccessful = valueModelPair.getF0().resetAndAppendAll(notYetEmitted);
            valueModelPair.setF1(appendSuccessful);
            anyValueModelsStillActive = anyValueModelsStillActive | appendSuccessful;
        }


        boolean anyTimeStampModelsStillActive = false;
        for (Pair<TimestampCompressionModel, Boolean> timestampModelPair : timestampModelPairs) {
            boolean appendSuccessful = timestampModelPair.getF0().resetAndAppendAll(notYetEmitted);
            timestampModelPair.setF1(appendSuccessful);
            anyTimeStampModelsStillActive = anyTimeStampModelsStillActive | appendSuccessful;
        }

        return anyValueModelsStillActive && anyTimeStampModelsStillActive;
    }

    public CompressionModel getBestCompressionModel() {
        List<ValueCompressionModel> valueModels = valueModelPairs.stream().map(Pair::getF0).collect(Collectors.toList());
        List<TimestampCompressionModel> timestampModels = timestampModelPairs.stream().map(Pair::getF0).collect(Collectors.toList());
        return modelPicker.findBestCompressionModel(valueModels, timestampModels);
    }

}
