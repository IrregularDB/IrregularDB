package segmentgenerator;

import compression.BaseModel;
import compression.timestamp.TimestampCompressionModel;
import compression.value.ValueCompressionModel;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Produces and caches blobs for all model combinations
 * Used to aid ModelPickerBruteForce
 */
public class ModelPickerBruteForceBlobBuffer {

    private final Map<BaseModel, Map<Integer, ByteBuffer>> compressionModelBlobBuffer;

    /**
     * Notice that the models will have reduceToSizeN called for each of them with the lowest size of the other type
     */
    public ModelPickerBruteForceBlobBuffer(List<ValueCompressionModel> valueModels, List<TimestampCompressionModel> timestampModels) {
        this.compressionModelBlobBuffer = new HashMap<>();

        // We use sets here to remove duplicates
        Set<Integer> valueModelLengths = valueModels.stream()
                .map(BaseModel::getLength)
                .collect(Collectors.toSet());
        Set<Integer> timestampModelsLength = timestampModels.stream()
                .map(BaseModel::getLength)
                .collect(Collectors.toSet());

        performCachingForList(valueModels, timestampModelsLength);
        performCachingForList(timestampModels, valueModelLengths);

    }

    private void performCachingForList(List<? extends BaseModel> models, Set<Integer> lengthsOfOther) {
        List<Integer> reverseSortedLengths = lengthsOfOther.stream()
                .sorted(Comparator.reverseOrder())
                .toList();
        for (BaseModel model : models) {
            cacheBlobForAllLengths(model, reverseSortedLengths);
        }
    }

    private void cacheBlobForAllLengths(BaseModel baseModel, List<Integer> reverseSortedUniqueLengths) {

        Map<Integer, ByteBuffer> lengthToByteBuffer = compressionModelBlobBuffer.computeIfAbsent(baseModel, model -> new HashMap<>());

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
    }

    public Optional<ByteBuffer> getBlobForModelWithLength(BaseModel baseModel, int length) {
        Map<Integer, ByteBuffer> integerByteBufferMap = compressionModelBlobBuffer.get(baseModel);
        if (integerByteBufferMap.containsKey(length)) {
            return Optional.of(integerByteBufferMap.get(length));
        } else {
            return Optional.empty();
        }
    }

}
