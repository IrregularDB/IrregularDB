package segmentgenerator;

import compression.BaseModel;
import compression.timestamp.TimestampCompressionModel;
import compression.timestamp.TimestampCompressionModelType;
import compression.value.ValueCompressionModel;
import compression.value.ValueCompressionModelType;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Produces and caches blobs for all model combinations
 * Used to aid ModelPickerBruteForce
 */
public class ModelPickerBruteForceBlobBuffer {
    private final Map<TimestampCompressionModelType, Map<Integer, ByteBuffer>> timestampModelsBlobBuffer;
    private final Map<ValueCompressionModelType, Map<Integer, ByteBuffer>> valueModelsBlobBuffer;

    /**
     * Notice that the models will have reduceToSizeN called for each of them with the lowest size of the other type
     */
    public ModelPickerBruteForceBlobBuffer(List<ValueCompressionModel> valueModels, List<TimestampCompressionModel> timestampModels) {
        this.timestampModelsBlobBuffer = new HashMap<>();
        this.valueModelsBlobBuffer = new HashMap<>();

        // We use sets here to remove duplicates
        Set<Integer> valueModelLengths = valueModels.stream()
                .map(BaseModel::getLength)
                .collect(Collectors.toSet());
        Set<Integer> timestampModelLengths = timestampModels.stream()
                .map(BaseModel::getLength)
                .collect(Collectors.toSet());

        performCachingForTimestampModels(timestampModels, valueModelLengths);
        performCachingForValueModels(valueModels, timestampModelLengths);

    }

    private void performCachingForTimestampModels(List<TimestampCompressionModel> timestampModels, Set<Integer> valueModelLengths) {
        List<Integer> reverseSortedLengths = valueModelLengths.stream()
                .sorted(Comparator.reverseOrder())
                .toList();

        for (TimestampCompressionModel model : timestampModels) {
            Map<Integer, ByteBuffer> lengthToByteBufferMap = getLengthToByteBufferMap(model, reverseSortedLengths);
            this.timestampModelsBlobBuffer.put(model.getTimestampCompressionModelType(), lengthToByteBufferMap);
        }
    }

    private void performCachingForValueModels(List<ValueCompressionModel> valueModels, Set<Integer> timestampModelLengths) {
        List<Integer> reverseSortedLengths = timestampModelLengths.stream()
                .sorted(Comparator.reverseOrder())
                .toList();

        for (ValueCompressionModel model : valueModels) {
            Map<Integer, ByteBuffer> lengthToByteBufferMap = getLengthToByteBufferMap(model, reverseSortedLengths);
            this.valueModelsBlobBuffer.put(model.getValueCompressionModelType(), lengthToByteBufferMap);
        }
    }

    private Map<Integer, ByteBuffer> getLengthToByteBufferMap(BaseModel baseModel, List<Integer> reverseSortedUniqueLengths) {
        Map<Integer, ByteBuffer> lengthToByteBuffer = new HashMap<>();

        lengthToByteBuffer.put(baseModel.getLength(), baseModel.getBlobRepresentation());

        for (Integer lengthOfOtherModel : reverseSortedUniqueLengths) {
            if (lengthOfOtherModel >= baseModel.getLength()) {
                continue;
            }

            baseModel.reduceToSizeN(lengthOfOtherModel);
            if (baseModel.canCreateByteBuffer()) {
                ByteBuffer blobRepresentation = baseModel.getBlobRepresentation();
                lengthToByteBuffer.put(lengthOfOtherModel, blobRepresentation);
            }
        }

        return lengthToByteBuffer;
    }

    public Optional<ByteBuffer> getBlobForTimestampModelWithLength(TimestampCompressionModelType modelType, int length) {
        Map<Integer, ByteBuffer> lengthToByteBufferMap = timestampModelsBlobBuffer.get(modelType);
        if (lengthToByteBufferMap.containsKey(length)) {
            return Optional.of(lengthToByteBufferMap.get(length));
        } else {
            return Optional.empty();
        }
    }

    public Optional<ByteBuffer> getBlobForValueModelWithLength(ValueCompressionModelType modelType, int length) {
        Map<Integer, ByteBuffer> lengthToByteBufferMap = valueModelsBlobBuffer.get(modelType);
        if (lengthToByteBufferMap.containsKey(length)) {
            return Optional.of(lengthToByteBufferMap.get(length));
        } else {
            return Optional.empty();
        }
    }

}
