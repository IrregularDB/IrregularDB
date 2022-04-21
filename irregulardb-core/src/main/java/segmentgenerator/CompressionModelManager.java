package segmentgenerator;

import records.CompressionModel;
import compression.timestamp.TimestampCompressionModel;
import compression.value.ValueCompressionModel;
import records.DataPoint;

import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CompressionModelManager {

    private final ModelPicker modelPicker;

    private List<ValueCompressionModel> activeValueModels;
    private List<TimestampCompressionModel> activeTimestampModels;

    private List<ValueCompressionModel> inactiveValueModels;
    private List<TimestampCompressionModel> inactiveTimestampModels;

    public CompressionModelManager(List<ValueCompressionModel> valueCompressionModels, List<TimestampCompressionModel> timestampCompressionModels) {
        this.activeValueModels = new ArrayList<>(valueCompressionModels);
        this.activeTimestampModels = new ArrayList<>(timestampCompressionModels);

        this.inactiveValueModels = new ArrayList<>();
        this.inactiveTimestampModels = new ArrayList<>();
        this.modelPicker = ModelPickerFactory.getModelPicker();//can be taken as a method parameter if we want
    }

    public boolean tryAppendDataPointToAllModels(DataPoint dataPoint) {
        // Partition models by append success
        List<ValueCompressionModel> valueModelsNowInactive = activeValueModels.stream()
                .filter(model -> !model.append(dataPoint))
                .toList();

        for (ValueCompressionModel inactiveValueModel : valueModelsNowInactive) {
            this.activeValueModels.remove(inactiveValueModel);
            this.inactiveValueModels.add(inactiveValueModel);
        }

        List<TimestampCompressionModel> timeModelsNowInactive = this.activeTimestampModels.stream()
                .filter(model -> !model.append(dataPoint))
                .toList();

        for (TimestampCompressionModel inactiveTimestampModel : timeModelsNowInactive) {
            this.activeTimestampModels.remove(inactiveTimestampModel);
            this.inactiveTimestampModels.add(inactiveTimestampModel);
        }
        return (!this.activeValueModels.isEmpty()) && (!this.activeTimestampModels.isEmpty());
    }

    public boolean resetAndTryAppendBuffer(List<DataPoint> notYetEmitted) {
        activeValueModels.addAll(inactiveValueModels);
        inactiveValueModels.clear();

        activeTimestampModels.addAll(inactiveTimestampModels);
        inactiveTimestampModels.clear();

        List<ValueCompressionModel> valueModelsToMakeInactive = new ArrayList<>();
        for (ValueCompressionModel model : activeValueModels) {
            if (!model.resetAndAppendAll(notYetEmitted)) {
                valueModelsToMakeInactive.add(model);
            }
        }
        for (ValueCompressionModel valueCompressionModel : valueModelsToMakeInactive) {
            activeValueModels.remove(valueCompressionModel);
            inactiveValueModels.add(valueCompressionModel);
        }

        List<TimestampCompressionModel> timeModelToMakeInactive = new ArrayList<>();
        for (TimestampCompressionModel model : activeTimestampModels) {
            if (!model.resetAndAppendAll(notYetEmitted)) {
                timeModelToMakeInactive.add(model);
            }
        }
        for (TimestampCompressionModel timestampCompressionModel : timeModelToMakeInactive) {
            activeTimestampModels.remove(timestampCompressionModel);
            inactiveTimestampModels.add(timestampCompressionModel);
        }

        return !activeValueModels.isEmpty() && !activeTimestampModels.isEmpty();
    }


    public CompressionModel getBestCompressionModel() {
        List<ValueCompressionModel> valueModels = Stream.concat(activeValueModels.stream(), inactiveValueModels.stream()).toList();
        List<TimestampCompressionModel> timeStampModels = Stream.concat(activeTimestampModels.stream(), inactiveTimestampModels.stream()).toList();
        return modelPicker.findBestCompressionModel(valueModels, timeStampModels);
    }
}
