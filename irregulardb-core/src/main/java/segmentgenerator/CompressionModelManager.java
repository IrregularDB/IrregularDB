package segmentgenerator;

import records.CompressionModel;
import compression.timestamp.TimestampCompressionModel;
import compression.value.ValueCompressionModel;
import records.DataPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class CompressionModelManager {

    private final ModelPicker modelPicker;

    private final List<ValueCompressionModel> activeValueModels;
    private final List<TimestampCompressionModel> activeTimestampModels;

    private final List<ValueCompressionModel> inactiveValueModels;
    private final List<TimestampCompressionModel> inactiveTimestampModels;

    public CompressionModelManager(List<ValueCompressionModel> valueCompressionModels, List<TimestampCompressionModel> timestampCompressionModels,
                                  ModelPicker modelPicker) {
        this.activeValueModels = new ArrayList<>(valueCompressionModels);
        this.activeTimestampModels = new ArrayList<>(timestampCompressionModels);

        this.inactiveValueModels = new ArrayList<>();
        this.inactiveTimestampModels = new ArrayList<>();
        this.modelPicker = modelPicker;
    }

    public boolean tryAppendDataPointToAllModels(DataPoint dataPoint) {
        // Partition models by append success
        List<ValueCompressionModel> valueModelsNowInactive = new ArrayList<>();
        for (ValueCompressionModel valueModel : activeValueModels){
            if (!valueModel.append(dataPoint)){
                valueModelsNowInactive.add(valueModel);
            }
        }
        makeValueModelsInactive(valueModelsNowInactive);

        List<TimestampCompressionModel> timeModelsNowInactive = new ArrayList<>();
        for (TimestampCompressionModel timeModel : activeTimestampModels){
            if (!timeModel.append(dataPoint)){
                timeModelsNowInactive.add(timeModel);
            }
        }
        makeTimestampModelsInactive(timeModelsNowInactive);

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
        makeValueModelsInactive(valueModelsToMakeInactive);

        List<TimestampCompressionModel> timeModelToMakeInactive = new ArrayList<>();
        for (TimestampCompressionModel model : activeTimestampModels) {
            if (!model.resetAndAppendAll(notYetEmitted)) {
                timeModelToMakeInactive.add(model);
            }
        }
        makeTimestampModelsInactive(timeModelToMakeInactive);

        return !activeValueModels.isEmpty() && !activeTimestampModels.isEmpty();
    }

    public CompressionModel getBestCompressionModel() {
        List<ValueCompressionModel> valueModels = Stream.concat(activeValueModels.stream(), inactiveValueModels.stream()).toList();
        List<TimestampCompressionModel> timeStampModels = Stream.concat(activeTimestampModels.stream(), inactiveTimestampModels.stream()).toList();
        return modelPicker.findBestCompressionModel(valueModels, timeStampModels);
    }

    private void makeValueModelsInactive(List<ValueCompressionModel> inactiveModels){
        for (ValueCompressionModel inactiveModel : inactiveModels){
            activeValueModels.remove(inactiveModel);
            inactiveValueModels.add(inactiveModel);
        }
    }

    private void makeTimestampModelsInactive(List<TimestampCompressionModel> inactiveModels){
        for (TimestampCompressionModel inactiveModel : inactiveModels){
            activeTimestampModels.remove(inactiveModel);
            inactiveTimestampModels.add(inactiveModel);
        }
    }

}
