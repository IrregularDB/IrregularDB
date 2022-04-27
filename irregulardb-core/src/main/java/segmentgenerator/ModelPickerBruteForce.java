package segmentgenerator;

import compression.BaseModel;
import compression.timestamp.TimestampCompressionModel;
import compression.timestamp.TimestampCompressionModelType;
import compression.value.ValueCompressionModel;
import compression.value.ValueCompressionModelType;
import records.CompressionModel;
import records.Pair;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ModelPickerBruteForce extends ModelPicker{
    private Map<TimestampCompressionModelType, Integer> timestampModelsLengths;
    private Map<ValueCompressionModelType, Integer> valueModelsLengths;
    private ModelPickerBruteForceBlobBuffer modelPickerBlobBuffer;

    @Override
    public CompressionModel findBestCompressionModel(List<ValueCompressionModel> valueModels, List<TimestampCompressionModel> timestampModels) {
        timestampModelsLengths = timestampModels.stream()
                .collect(Collectors.toMap(TimestampCompressionModel::getTimestampCompressionModelType, BaseModel::getLength));

        valueModelsLengths = valueModels.stream()
                .collect(Collectors.toMap(ValueCompressionModel::getValueCompressionModelType, BaseModel::getLength));

        modelPickerBlobBuffer = new ModelPickerBruteForceBlobBuffer(valueModels, timestampModels);

        List<Pair<TimestampCompressionModelType, ValueCompressionModelType>> allPairs = getAllPairs(valueModels,timestampModels);
        Pair<TimestampCompressionModelType, ValueCompressionModelType> bestPair = findBestFromAllPairs(allPairs);

        int minLength = Math.min(timestampModelsLengths.get(bestPair.f0()), valueModelsLengths.get(bestPair.f1()));

        return new CompressionModel(
                bestPair.f1(),
                modelPickerBlobBuffer.getBlobForValueModelWithLength(bestPair.f1(), minLength).orElseThrow(),
                bestPair.f0(),
                modelPickerBlobBuffer.getBlobForTimestampModelWithLength(bestPair.f0(), minLength).orElseThrow(),
                minLength
        );

    }

    private Pair<TimestampCompressionModelType, ValueCompressionModelType> findBestFromAllPairs(List<Pair<TimestampCompressionModelType, ValueCompressionModelType>> allPairs) {
        Pair<TimestampCompressionModelType,ValueCompressionModelType> bestPair = null;
        double currentBestBytesPerDataPoint = Double.MAX_VALUE;

        for (var pair : allPairs) {
            int minLength = Math.min(timestampModelsLengths.get(pair.f0()), valueModelsLengths.get(pair.f1()));
            Optional<ByteBuffer> timestampBlob = modelPickerBlobBuffer.getBlobForTimestampModelWithLength(pair.f0(), minLength);
            Optional<ByteBuffer> valueBlob = modelPickerBlobBuffer.getBlobForValueModelWithLength(pair.f1(), minLength);

            if (timestampBlob.isPresent() && valueBlob.isPresent()) {
                double bytesPerDataPoint = calculateAmountBytesPerDataPoint(timestampBlob.get().capacity(), minLength)
                        + calculateAmountBytesPerDataPoint(valueBlob.get().capacity(), minLength);
                if (bytesPerDataPoint < currentBestBytesPerDataPoint) {
                    bestPair = pair;
                    currentBestBytesPerDataPoint = bytesPerDataPoint;
                }
            }
        }
        if(bestPair == null) {
            throw new RuntimeException("All model-pairs where illegal in the brute force model picker, which should not happen");
        }
        return bestPair;
    }

    private List<Pair<TimestampCompressionModelType, ValueCompressionModelType>> getAllPairs(List<ValueCompressionModel> valueModels, List<TimestampCompressionModel> timestampModels) {
        List<Pair<TimestampCompressionModelType, ValueCompressionModelType>> result = new ArrayList<>();
        for (TimestampCompressionModel timestampModel : timestampModels) {
            for (ValueCompressionModel valueModel : valueModels) {
                result.add(new Pair<>(timestampModel.getTimestampCompressionModelType(), valueModel.getValueCompressionModelType()));
            }
        }
        return result;
    }
}
