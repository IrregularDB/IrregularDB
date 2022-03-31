package compression;

import compression.timestamp.*;
import compression.value.*;
import config.ConfigProperties;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static compression.timestamp.TimeStampCompressionModelType.*;
import static compression.value.ValueCompressionModelType.*;
import static compression.value.ValueCompressionModelType.PMC_MEAN;

public class CompressionModelFactory {

    static ConfigProperties config = ConfigProperties.getInstance();


    public static List<TimeStampCompressionModel> getTimestampCompressionModels(){
        List<TimeStampCompressionModelType> timeStampModelTypes =  config.getTimeStampModels();
        final float timestampModelErrorBound = config.getTimeStampModelErrorBound();

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
                return new GorillaValueCompressionModel(ConfigProperties.getInstance().getValueModelLengthBound());
            case SWING:
                return new SwingValueCompressionModel(errorBound);
            default:
                throw new RuntimeException("Type not defined");
        }
    }


    private static TimeStampCompressionModel getTimestampCompressionModelByType(TimeStampCompressionModelType timeStampCompressionModelType, float errorBound) {
        switch (timeStampCompressionModelType){
            case REGULAR:
                return new RegularTimeStampCompressionModel(errorBound);
            case DELTADELTA:
                return new DeltaDeltaTimeStampCompression();
            case SIDIFF:
                return new SIDiffTimeStampCompressionModel(errorBound);
            default:
                throw new RuntimeException("Type not defined");
        }
    }
}
