package segmentgenerator;

import compression.BaseModel;
import compression.timestamp.TimestampCompressionModel;
import compression.value.ValueCompressionModel;
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
    private Map<TimestampCompressionModel, Integer> timestampModelsLengths;
    private Map<ValueCompressionModel, Integer> valueModelsLengths;
    private ModelPickerBruteForceBlobBuffer modelPickerBlobBuffer;

    @Override
    public CompressionModel findBestCompressionModel(List<ValueCompressionModel> valueModels, List<TimestampCompressionModel> timestampModels) {
        timestampModelsLengths = timestampModels.stream()
                .collect(Collectors.toMap(model -> model, BaseModel::getLength));

        valueModelsLengths = valueModels.stream()
                .collect(Collectors.toMap(model -> model, BaseModel::getLength));

        modelPickerBlobBuffer = new ModelPickerBruteForceBlobBuffer(valueModels, timestampModels);

        List<Pair<TimestampCompressionModel, ValueCompressionModel>> allPairs = getAllPairs(valueModels,timestampModels);
        Pair<TimestampCompressionModel, ValueCompressionModel> bestPair = findBestFromAllPairs(allPairs);

        int minLength = Math.min(timestampModelsLengths.get(bestPair.f0()), valueModelsLengths.get(bestPair.f1()));

        return new CompressionModel(
                bestPair.f1().getValueCompressionModelType(),
                modelPickerBlobBuffer.getBlobForModelWithLength(bestPair.f1(), minLength).orElseThrow(),
                bestPair.f0().getTimestampCompressionModelType(),
                modelPickerBlobBuffer.getBlobForModelWithLength(bestPair.f0(), minLength).orElseThrow(),
                minLength
        );

    }

    private Pair<TimestampCompressionModel, ValueCompressionModel> findBestFromAllPairs(List<Pair<TimestampCompressionModel, ValueCompressionModel>> allPairs) {
        Pair<TimestampCompressionModel,ValueCompressionModel> bestPair = null;
        double currentBestBytePerDataPoint = Double.MAX_VALUE;

        for (Pair<TimestampCompressionModel, ValueCompressionModel> pair : allPairs) {
            int minLength = Math.min(timestampModelsLengths.get(pair.f0()), valueModelsLengths.get(pair.f1()));
            Optional<ByteBuffer> timestampBlob = modelPickerBlobBuffer.getBlobForModelWithLength(pair.f0(), minLength);
            Optional<ByteBuffer> valueBlob = modelPickerBlobBuffer.getBlobForModelWithLength(pair.f1(), minLength);

            if (timestampBlob.isPresent() && valueBlob.isPresent()) {
                double bytesPerDataPoint = calculateAmountBytesPerDataPoint(timestampBlob.get().capacity(), minLength)
                        + calculateAmountBytesPerDataPoint(valueBlob.get().capacity(), minLength);
                if (bytesPerDataPoint < currentBestBytePerDataPoint) {
                    bestPair = pair;
                    currentBestBytePerDataPoint = bytesPerDataPoint;
                }
            }
        }
        if(bestPair == null) {
            throw new RuntimeException("All model-pairs where illegal in the brute force model picker, which should not happen");
        }
        return bestPair;
    }

    private List<Pair<TimestampCompressionModel, ValueCompressionModel>> getAllPairs(List<ValueCompressionModel> valueModels, List<TimestampCompressionModel> timestampModels) {
        List<Pair<TimestampCompressionModel, ValueCompressionModel>> result = new ArrayList<>();
        for (TimestampCompressionModel timestampModel : timestampModels) {
            for (ValueCompressionModel valueModel : valueModels) {
                result.add(new Pair<>(timestampModel, valueModel));
            }
        }
        return result;
    }



}
