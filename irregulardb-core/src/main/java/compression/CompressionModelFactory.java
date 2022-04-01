package compression;

import compression.timestamp.*;
import compression.value.*;
import config.ConfigProperties;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;


public class CompressionModelFactory {

    static ConfigProperties config = ConfigProperties.getInstance();


    public static List<TimestampCompressionModel> getTimestampCompressionModels(String tag){
        List<TimestampCompressionModelType> timestampModelTypes =  config.getTimestampModels();
        final Integer timestampModelThreshold = config.getTimeStampThresholdForTimeSeriesTag(tag);

        return getCompressionModels(timestampModelTypes, (modelType) -> getTimestampCompressionModelByType(modelType, timestampModelThreshold));
    }


    public static List<ValueCompressionModel> getValueCompressionModels(String tag) {
        List<ValueCompressionModelType> valueModelTypes =  config.getValueModels();
        final Float valueModelErrorBound = config.getValueErrorBoundForTimeSeriesTag(tag);

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
                return new GorillaValueCompressionModel();
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
                return new DeltaDeltaTimestampCompressionModel(threshold);
            case SIDIFF:
                return new SIDiffTimestampCompressionModel(threshold);
            default:
                throw new RuntimeException("Type not defined");
        }
    }
}
