package segmentgenerator;

import compression.CompressionModel;
import compression.timestamp.TimestampCompressionModel;
import compression.value.ValueCompressionModel;
import records.DataPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompressionModelManager {

    private List<ValueCompressionModel> activeValueModels;
    private List<TimestampCompressionModel> activeTimestampModels;

    private List<ValueCompressionModel> inactiveValueModels;
    private List<TimestampCompressionModel> inactiveTimestampModels;

    public CompressionModelManager(List<ValueCompressionModel> valueCompressionModels, List<TimestampCompressionModel> timestampCompressionModels) {
        this.activeValueModels = new ArrayList<>(valueCompressionModels);
        this.activeTimestampModels = new ArrayList<>(timestampCompressionModels);

        this.inactiveValueModels = new ArrayList<>();
        this.inactiveTimestampModels = new ArrayList<>();
    }

    public boolean tryAppendDataPointToAllModels(DataPoint dataPoint) {
        // Partition models by append success
        Map<Boolean, List<ValueCompressionModel>> valueModelsAppended = activeValueModels.stream()
                .collect(Collectors.partitioningBy(valueModel -> valueModel.append(dataPoint)));

        // Update local lists in active and inactive
        this.activeValueModels = valueModelsAppended.get(true);
        this.inactiveValueModels.addAll(valueModelsAppended.get(false));

        // Same for time stamp models
        Map<Boolean, List<TimestampCompressionModel>> timestampModelAppended = activeTimestampModels.stream()
                .collect(Collectors.partitioningBy(timestampModel -> timestampModel.append(dataPoint)));

        this.activeTimestampModels = timestampModelAppended.get(true);
        this.inactiveTimestampModels.addAll(timestampModelAppended.get(false));

        return (!this.activeValueModels.isEmpty()) && (!this.activeTimestampModels.isEmpty());
    }

    public boolean resetAndTryAppendBuffer(List<DataPoint> notYetEmitted) {
        this.activeValueModels.addAll(inactiveValueModels);
        this.activeTimestampModels.addAll(inactiveTimestampModels);

        Map<Boolean, List<ValueCompressionModel>> valueModelsAppended = activeValueModels.stream()
                .collect(Collectors.partitioningBy(valueModel -> valueModel.resetAndAppendAll(notYetEmitted)));
        // Update local lists in active and inactive
        this.activeValueModels = valueModelsAppended.get(true);
        this.inactiveValueModels = valueModelsAppended.get(false);

        // Same for time stamp models
        Map<Boolean, List<TimestampCompressionModel>> timestampModelAppended = activeTimestampModels.stream()
                .collect(Collectors.partitioningBy(timestampModel -> timestampModel.resetAndAppendAll(notYetEmitted)));

        this.activeTimestampModels = timestampModelAppended.get(true);
        this.inactiveTimestampModels = timestampModelAppended.get(false);

        return !activeValueModels.isEmpty() && !activeTimestampModels.isEmpty();
    }


    public CompressionModel getBestCompressionModel() {
        List<ValueCompressionModel> valueModels = Stream.concat(activeValueModels.stream(), inactiveValueModels.stream()).toList();
        List<TimestampCompressionModel> timeStampModels = Stream.concat(activeTimestampModels.stream(), inactiveTimestampModels.stream()).toList();
        return ModelPicker.findBestCompressionModel(valueModels, timeStampModels);
    }
}
