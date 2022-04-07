package segmentgenerator;

import compression.BaseModel;
import compression.CompressionModelFactory;
import compression.timestamp.TimestampCompressionModel;
import compression.timestamp.TimestampCompressionModelType;
import compression.value.ValueCompressionModel;
import compression.value.ValueCompressionModelType;
import config.ConfigProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import records.DataPoint;
import records.Pair;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

class ModelPickerBruteForceBlobBufferTest {

    @BeforeAll
    public static void setup(){
        ConfigProperties.isTest = true;
    }

    @Test
    void getBlobForModelWithLength() {
        List<DataPoint> testDataPoints = createTestDataPoints();
        List<ValueCompressionModel> valueModels = CompressionModelFactory.getValueCompressionModels("testTag1");
        List<TimestampCompressionModel> timestampModels = CompressionModelFactory.getTimestampCompressionModels("testTag1");

        valueModels.forEach(model -> model.resetAndAppendAll(testDataPoints));
        timestampModels.forEach(model -> model.resetAndAppendAll(testDataPoints));

        Map<TimestampCompressionModel, Integer> timestampModelsLengths = timestampModels.stream()
                .collect(Collectors.toMap(model -> model, BaseModel::getLength));

        Map<ValueCompressionModel, Integer> valueModelsLengths = valueModels.stream()
                .collect(Collectors.toMap(model -> model, BaseModel::getLength));

        ModelPickerBruteForceBlobBuffer modelPickerBruteForceBlobBuffer = new ModelPickerBruteForceBlobBuffer(valueModels, timestampModels);

        List<Pair<TimestampCompressionModel, ValueCompressionModel>> allPairs = getAllPairs(timestampModels, valueModels);

        for (Pair<TimestampCompressionModel, ValueCompressionModel> pair : allPairs) {
            Integer timestampLength = timestampModelsLengths.get(pair.f0());
            Integer valueLength = valueModelsLengths.get(pair.f1());
            int minLength = Math.min(timestampLength,valueLength);
            Optional<ByteBuffer> timestampBlob = modelPickerBruteForceBlobBuffer.getBlobForModelWithLength(pair.f0(), minLength);
            Optional<ByteBuffer> valueBlob = modelPickerBruteForceBlobBuffer.getBlobForModelWithLength(pair.f1(), minLength);

            if (timestampBlob.isPresent() && valueBlob.isPresent()) {
                System.out.println("valid pair: <" + pair.f0().getTimestampCompressionModelType().name() + "[" + timestampModelsLengths.get(pair.f0()) + "] " + pair.f1().getValueCompressionModelType().name() + "[" + valueModelsLengths.get(pair.f1()) + "]" + ">" + "; size=" + minLength);
                Assertions.assertTrue(correctValidPair(pair));
            } else {
                System.out.println("Invalid pair: <" + pair.f0().getTimestampCompressionModelType().name() + "[" + timestampModelsLengths.get(pair.f0()) + "] " + pair.f1().getValueCompressionModelType().name() + "[" + valueModelsLengths.get(pair.f1()) + "]" + ">");
                Assertions.assertTrue(correctInvalidPair(pair));
            }
        }

    }

    private boolean correctInvalidPair(Pair<TimestampCompressionModel, ValueCompressionModel> pair){
        return (pair.f0().getTimestampCompressionModelType() == TimestampCompressionModelType.REGULAR &&
                pair.f1().getValueCompressionModelType() == ValueCompressionModelType.PMC_MEAN)
                ||
                (pair.f0().getTimestampCompressionModelType() == TimestampCompressionModelType.SIDIFF &&
                pair.f1().getValueCompressionModelType() == ValueCompressionModelType.PMC_MEAN);
    }

    private boolean correctValidPair(Pair<TimestampCompressionModel,ValueCompressionModel> pair){
        return !correctInvalidPair(pair);
    }

    private List<Pair<TimestampCompressionModel, ValueCompressionModel>> getAllPairs(List<TimestampCompressionModel> timestampModels, List<ValueCompressionModel> valueModels) {
        List<Pair<TimestampCompressionModel, ValueCompressionModel>> result = new ArrayList<>();
        for (TimestampCompressionModel timestampModel : timestampModels) {
            for (ValueCompressionModel valueModel : valueModels) {
                result.add(new Pair<>(timestampModel, valueModel));
            }
        }
        return result;
    }


    private List<DataPoint> createTestDataPoints() {
        return List.of(
                new DataPoint(10,10),
                new DataPoint(12,11),
                new DataPoint(14,13),
                new DataPoint(15,14),
                new DataPoint(17,15),
                new DataPoint(20,26),
                new DataPoint(25,51)
        );
    }
}