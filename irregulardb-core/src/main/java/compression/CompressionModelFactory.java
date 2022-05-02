package compression;

import compression.timestamp.*;
import compression.value.*;
import config.ConfigProperties;
import segmentgenerator.CompressionModelManager;
import segmentgenerator.ModelPicker;
import segmentgenerator.ModelPickerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;


public class CompressionModelFactory {
    private static final ConfigProperties config = ConfigProperties.getInstance();
    private static final int lengthBound = config.getModelLengthBound();
    private static final List<TimestampCompressionModelType> timestampModelTypes =  config.getTimestampModels();
    private static final List<ValueCompressionModelType> valueModelTypes =  config.getValueModels();

    public static List<TimestampCompressionModel> getTimestampCompressionModels(String tag){
        final Integer timestampModelThreshold = config.getTimeStampThresholdForTimeSeriesTag(tag);
        return getCompressionModels(timestampModelTypes, (modelType) -> getTimestampCompressionModelByType(modelType, timestampModelThreshold));
    }

    public static List<ValueCompressionModel> getValueCompressionModels(String tag) {
        final Float valueModelErrorBound = config.getValueErrorBoundForTimeSeriesTag(tag);
        final Integer threshold = config.getTimeStampThresholdForTimeSeriesTag(tag);
        List<ValueCompressionModelType> modelTypes = new ArrayList<>(valueModelTypes);
        if (threshold > 0) {
            boolean wasSwingRemoved = modelTypes.remove(ValueCompressionModelType.SWING);
            if (wasSwingRemoved) {
                System.out.println("Disabled SWING value model for the tag: " + tag + ", because its threshold is greater than zero.");
            }
        }
        return getCompressionModels(valueModelTypes, (modelType) -> CompressionModelFactory.getValueCompressionModelByType(modelType, valueModelErrorBound));
    }


    private static <E,T> List<E> getCompressionModels(List<T> modelTypes, Function<T, E> getModelInstance){
        return modelTypes.stream()
                .map(getModelInstance)
                .collect(Collectors.toList());
    }

    private static ValueCompressionModel getValueCompressionModelByType(ValueCompressionModelType valueCompressionModelType, float errorBound) {
        switch (valueCompressionModelType) {
            case PMC_MEAN:
                return new PMCMeanValueCompressionModel(errorBound);
            case GORILLA:
                return new GorillaValueCompressionModel(lengthBound);
            case SWING:
                return new SwingValueCompressionModel(errorBound);
            default:
                throw new RuntimeException("Type not defined");
        }
    }


    private static TimestampCompressionModel getTimestampCompressionModelByType(TimestampCompressionModelType timestampCompressionModelType, Integer threshold) {
        switch (timestampCompressionModelType){
            case REGULAR:
                return new RegularTimestampCompressionModel(threshold);
            case DELTADELTA:
                return new DeltaDeltaTimestampCompressionModel(threshold,lengthBound);
            case SIDIFF:
                return new SIDiffTimestampCompressionModel(threshold,lengthBound);
            default:
                throw new RuntimeException("Type not defined");
        }
    }
}
