package segmentgenerator;

import compression.CompressionModel;
import compression.timestamp.TimeStampCompressionModel;
import compression.value.ValueCompressionModel;
import records.DataPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CompressionModelManager {

    private List<ValueCompressionModel> activeValueModels;
    private List<TimeStampCompressionModel> activeTimeStampModels;

    private List<ValueCompressionModel> inactiveValueModels;
    private List<TimeStampCompressionModel> inactiveTimestampModels;

    private final ModelPicker modelPicker;

    public CompressionModelManager(List<ValueCompressionModel> valueCompressionModels, List<TimeStampCompressionModel> timeStampCompressionModels) {
        this.activeValueModels = new ArrayList<>(valueCompressionModels);
        this.activeTimeStampModels = new ArrayList<>(timeStampCompressionModels);

        this.inactiveValueModels = new ArrayList<>();
        this.inactiveTimestampModels = new ArrayList<>();

        this.modelPicker = new ModelPicker(valueCompressionModels, timeStampCompressionModels);
    }

    public boolean tryAppendDataPointToAllModels(DataPoint dataPoint) {
        // Partition models by append success
        Map<Boolean, List<ValueCompressionModel>> valueModelsAppended = activeValueModels.stream()
                .collect(Collectors.partitioningBy(valueModel -> valueModel.append(dataPoint.value())));

        // Update local lists in active and inactive
        this.activeValueModels = valueModelsAppended.get(true);
        this.inactiveValueModels.addAll(valueModelsAppended.get(false));

        // Same for time stamp models
        Map<Boolean, List<TimeStampCompressionModel>> timeStampModelAppended = activeTimeStampModels.stream()
                .collect(Collectors.partitioningBy(timeStampModel -> timeStampModel.append(dataPoint.timestamp())));

        this.activeTimeStampModels = timeStampModelAppended.get(true);
        this.inactiveTimestampModels.addAll(timeStampModelAppended.get(false));

        return (!this.activeValueModels.isEmpty()) && (!this.activeTimeStampModels.isEmpty());
    }

    public boolean resetAndTryAppendBuffer(List<DataPoint> notYetEmitted) {
        this.activeValueModels.addAll(inactiveValueModels);
        this.activeTimeStampModels.addAll(inactiveTimestampModels);

        this.inactiveValueModels = new ArrayList<>();
        this.inactiveTimestampModels = new ArrayList<>();

        boolean anySuccessValue = this.activeValueModels.stream()
                .map((valueModel) -> valueModel.resetAndAppendAll(notYetEmitted))
                .anyMatch(item -> true);

        boolean anySuccessTimeStamp = this.activeTimeStampModels.stream()
                .map((timestampModel) -> timestampModel.resetAndAppendAll(notYetEmitted))
                .anyMatch(item -> true);

        return anySuccessValue && anySuccessTimeStamp;
    }


    public CompressionModel getBestCompressionModel() {
        return modelPicker.findBestCompressionModel();
    }
}
