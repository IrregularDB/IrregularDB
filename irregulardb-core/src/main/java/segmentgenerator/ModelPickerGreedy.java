package segmentgenerator;

import compression.encoding.BucketEncoding;
import compression.timestamp.TimestampCompressionModel;
import compression.value.TimestampCompressionModelsWrapper;
import compression.value.ValueCompressionModel;
import compression.value.ValueCompressionModelsWrapper;
import config.ConfigProperties;
import records.CompressionModel;

import java.util.Comparator;
import java.util.List;

public class ModelPickerGreedy extends ModelPicker {

    private final double gorillaBestBytePerDatapoint = getGorillaBestBytePerDatapoint();
    private final double siDiffAndDeltaDeltaBestBytePerDataPoint = getSiDiffDeltaDeltaBestBytesPerDatapoint();
    private final int lengthBound = ConfigProperties.getInstance().getModelLengthBound();

    private double getGorillaBestBytePerDatapoint() {
        //store 1 float as an int e.g. 32 bits, + 1 bit per value + 1 value that is not the same as the others
        int bitsForGorillaValueNotSame = 2 + 4 + 5 + 1;//lowest possible amount of bits for a gorilla value that is not the same as the rest
        int bitsUsedForGorilla = Integer.SIZE + (lengthBound - 2) + bitsForGorillaValueNotSame;

        double bytesUsedByGorilla = Math.ceil(bitsUsedForGorilla / (double) Byte.SIZE);

        return (bytesUsedByGorilla + overheadPerModel) / lengthBound;
    }

    private double getSiDiffDeltaDeltaBestBytesPerDatapoint() {

        int controlBits = 2;
        int signBits = 1;
        int smallestNonZeroBucketSizeInBits = BucketEncoding.getSmallestNonZeroBucketSizeInBits();
        int bitsForSmallesBucketSize = smallestNonZeroBucketSizeInBits + controlBits + signBits;
        int bitsUsedByModel = 2 * bitsForSmallesBucketSize + (lengthBound - 2) * controlBits; //TODO if simon no fix signed encoder add signBit
        double bytesUsedByModel = Math.ceil(bitsUsedByModel / (double) Byte.SIZE);

        return (bytesUsedByModel) / lengthBound;
    }

    @Override
    public CompressionModel findBestCompressionModel(List<ValueCompressionModel> valueCompressionModels, List<TimestampCompressionModel> timestampCompressionModels) {
        ValueCompressionModel bestValueCompressionModel = getBestValueModel(valueCompressionModels);
        TimestampCompressionModel bestTimestampCompressionModel = getBestTimeStampModel(timestampCompressionModels);

        if (bestValueCompressionModel.getLength() < bestTimestampCompressionModel.getLength()) {
            bestValueCompressionModel.reduceToSizeN(bestTimestampCompressionModel.getLength());
        } else {
            bestTimestampCompressionModel.reduceToSizeN(bestValueCompressionModel.getLength());
        }
        return new CompressionModel(
                bestValueCompressionModel.getValueCompressionModelType(),
                bestValueCompressionModel.getBlobRepresentation(),
                bestTimestampCompressionModel.getTimestampCompressionModelType(),
                bestTimestampCompressionModel.getBlobRepresentation(),
                bestTimestampCompressionModel.getLength()//could just as well be the value model length
        );
    }

    protected ValueCompressionModel getBestValueModel(List<ValueCompressionModel> valueCompressionModelsList) {
        //Can we ignore gorilla?
        ValueCompressionModelsWrapper valueCompressionModels = new ValueCompressionModelsWrapper(valueCompressionModelsList);

        performShortCircutingValueModels(valueCompressionModelsList, valueCompressionModels);

        return valueCompressionModelsList.stream()
                .min(Comparator.comparing(this::calculateAmountBytesPerDataPoint))
                .orElseThrow(() -> new RuntimeException("In ModelPicker:getBestValueModel() - models list.empty should not happen"));
    }

    private void performShortCircutingValueModels(List<ValueCompressionModel> valueCompressionModelsList, ValueCompressionModelsWrapper valueCompressionModels) {
        boolean gorillaRemoved = false;
        if (valueCompressionModels.getPmcMean() != null) {
            double pmcMeanBytesPerDataPoint = calculateAmountBytesPerDataPoint(valueCompressionModels.getPmcMean());
            if (pmcMeanBytesPerDataPoint <= gorillaBestBytePerDatapoint) {
                valueCompressionModelsList.remove(valueCompressionModels.getGorilla());
                gorillaRemoved = true;
            }
        }
        if (valueCompressionModels.getSwing() != null && !gorillaRemoved) {
            double swingBytesPerDataPoint = calculateAmountBytesPerDataPoint(valueCompressionModels.getSwing());
            if (swingBytesPerDataPoint <= gorillaBestBytePerDatapoint) {
                valueCompressionModelsList.remove(valueCompressionModels.getGorilla());
            }
        }
    }

    protected TimestampCompressionModel getBestTimeStampModel(List<TimestampCompressionModel> timestampCompressionModelsList) {
        //Can we ignore SIDiff or DeltaDelta
        TimestampCompressionModelsWrapper timestampCompressionModelsWrapper = new TimestampCompressionModelsWrapper(timestampCompressionModelsList);

        performShortCircutingTimestampModels(timestampCompressionModelsList, timestampCompressionModelsWrapper);

        return timestampCompressionModelsList.stream()
                .min(Comparator.comparing(this::calculateAmountBytesPerDataPoint))
                .orElseThrow(() -> new RuntimeException("In ModelPicker:getBestTimeStampModel() - Should not happen"));
    }

    private void performShortCircutingTimestampModels(List<TimestampCompressionModel> timestampCompressionModelsList, TimestampCompressionModelsWrapper timestampCompressionModelsWrapper) {
        if (timestampCompressionModelsWrapper.getRegular() != null) {
            double bytesPerDataPointRegular = calculateAmountBytesPerDataPoint(timestampCompressionModelsWrapper.getRegular());
            if (bytesPerDataPointRegular < siDiffAndDeltaDeltaBestBytePerDataPoint) {
                if (timestampCompressionModelsWrapper.getSiDiff() != null) {
                    timestampCompressionModelsList.remove(timestampCompressionModelsWrapper.getSiDiff());
                }
                if (timestampCompressionModelsWrapper.getDeltaDelta() != null) {
                    timestampCompressionModelsList.remove(timestampCompressionModelsWrapper.getDeltaDelta());
                }
            }
        }
    }

}
