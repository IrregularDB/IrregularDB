package segmentgenerator;

import compression.BaseModel;
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

    private final List<ValueCompressionModel> activeValueModels;
    private final List<TimestampCompressionModel> activeTimestampModels;

    private final List<ValueCompressionModel> inactiveValueModels;
    private final List<TimestampCompressionModel> inactiveTimestampModels;

    public CompressionModelManager(List<ValueCompressionModel> valueCompressionModels, List<TimestampCompressionModel> timestampCompressionModels) {
        this.activeValueModels = new ArrayList<>(valueCompressionModels);
        this.activeTimestampModels = new ArrayList<>(timestampCompressionModels);

        this.inactiveValueModels = new ArrayList<>();
        this.inactiveTimestampModels = new ArrayList<>();
        this.modelPicker = ModelPickerFactory.getModelPicker();//can be taken as a method parameter if we want
    }

    public boolean tryAppendDataPointToAllModels(DataPoint dataPoint) {
        // Partition models by append success

        List<BaseModel> valueModelsNowInactive = new ArrayList<>();

        for (ValueCompressionModel valueModel : activeValueModels){
            if (!valueModel.append(dataPoint)){
                valueModelsNowInactive.add(valueModel);
            }
        }

        makeModelsInactive(valueModelsNowInactive);

        List<BaseModel> timeModelsNowInactive = new ArrayList<>();

        for (TimestampCompressionModel timeModel : activeTimestampModels){
            if (!timeModel.append(dataPoint)){
                timeModelsNowInactive.add(timeModel);
            }
        }

        makeModelsInactive(timeModelsNowInactive);

        return (!this.activeValueModels.isEmpty()) && (!this.activeTimestampModels.isEmpty());
    }

    public boolean resetAndTryAppendBuffer(List<DataPoint> notYetEmitted) {
        activeValueModels.addAll(inactiveValueModels);
        inactiveValueModels.clear();

        activeTimestampModels.addAll(inactiveTimestampModels);
        inactiveTimestampModels.clear();

        List<BaseModel> valueModelsToMakeInactive = new ArrayList<>();
        for (ValueCompressionModel model : activeValueModels) {
            if (!model.resetAndAppendAll(notYetEmitted)) {
                valueModelsToMakeInactive.add(model);
            }
        }

        makeModelsInactive(valueModelsToMakeInactive);

        List<BaseModel> timeModelToMakeInactive = new ArrayList<>();
        for (TimestampCompressionModel model : activeTimestampModels) {
            if (!model.resetAndAppendAll(notYetEmitted)) {
                timeModelToMakeInactive.add(model);
            }
        }

        makeModelsInactive(timeModelToMakeInactive);

        return !activeValueModels.isEmpty() && !activeTimestampModels.isEmpty();
    }


    public CompressionModel getBestCompressionModel() {
        List<ValueCompressionModel> valueModels = Stream.concat(activeValueModels.stream(), inactiveValueModels.stream()).toList();
        List<TimestampCompressionModel> timeStampModels = Stream.concat(activeTimestampModels.stream(), inactiveTimestampModels.stream()).toList();
        return modelPicker.findBestCompressionModel(valueModels, timeStampModels);
    }

    private void makeModelsInactive(List<BaseModel> inactiveModels){
        for (BaseModel inactiveModel : inactiveModels){
            if (inactiveModel instanceof ValueCompressionModel) {
                activeValueModels.remove(inactiveModel);
                inactiveValueModels.add((ValueCompressionModel) inactiveModel);
            } else if (inactiveModel instanceof TimestampCompressionModel) {
                activeTimestampModels.remove(inactiveModel);
                inactiveTimestampModels.add((TimestampCompressionModel) inactiveModel);
            }
        }
    }
}
