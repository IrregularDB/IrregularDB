package segmentgenerator;

import compression.BaseModel;
import compression.timestamp.TimestampCompressionModel;
import compression.value.ValueCompressionModel;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class ModelPickerBruteForceBlobBuffer {

    private final Map<BaseModel, Map<Integer, ByteBuffer>> compressionModelBlobBuffer;

    /**
     * Notive that the models will have reduceToSizeN called for each of them with the lowest size of the other type
     */
    public ModelPickerBruteForceBlobBuffer(List<ValueCompressionModel> valueModels, List<TimestampCompressionModel> timestampModels) {
        this.compressionModelBlobBuffer = new HashMap<>();

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

        for (Integer reverseSortedUniqueLength : reverseSortedUniqueLengths) {
            if (reverseSortedUniqueLength >= baseModel.getLength()) {
                continue;
            }

            baseModel.reduceToSizeN(reverseSortedUniqueLength);
            if (baseModel.canCreateByteBuffer()) {
                ByteBuffer blobRepresentation = baseModel.getBlobRepresentation();
                lengthToByteBuffer.computeIfAbsent(reverseSortedUniqueLength, length -> blobRepresentation);
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
