package compression;

import compression.timestamp.RegularTimeStampCompressionModel;
import compression.timestamp.TimeStampCompressionModel;
import compression.timestamp.TimeStampCompressionModelType;
import compression.value.PMCMeanValueCompressionModel;
import compression.value.SwingValueCompressionModel;
import compression.value.ValueCompressionModel;
import compression.value.ValueCompressionModelType;
import config.ConfigProperties;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class CompressionModelFactory {

    static ConfigProperties config = ConfigProperties.INSTANCE;


    public static List<TimeStampCompressionModel> getTimestampCompressionModels(){
        List<TimeStampCompressionModelType> timeStampModelTypes =  config.getTimeStampModels();
        final double timestampModelErrorBound = config.getTimeStampModelErrorBound();

        return getCompressionModels(timeStampModelTypes, (modelType) -> getTimestampCompressionModelByType(modelType, timestampModelErrorBound));
    }


    public static List<ValueCompressionModel> getValueCompressionModels() {
        List<ValueCompressionModelType> valueModelTypes =  config.getValueModels();
        final float valueModelErrorBound = config.getValueModelErrorBound();

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
                //TODO
            case SWING:
                return new SwingValueCompressionModel(errorBound);
            default:
                throw new RuntimeException("Type not defined");
        }
    }


    private static TimeStampCompressionModel getTimestampCompressionModelByType(TimeStampCompressionModelType timeStampCompressionModelType, double errorBound) {
        switch (timeStampCompressionModelType){
            case REGULAR:
                return new RegularTimeStampCompressionModel(errorBound);
            case BASEDELTA:
                //TODO
            case DELTAPAIRS:
                //TODO
            case RECOMPUTESI:
                //TODO
            default:
                throw new RuntimeException("Type not defined");
        }
    }
}
