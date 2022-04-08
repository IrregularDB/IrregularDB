package segmentgenerator;

import compression.encoding.BucketEncoding;
import compression.timestamp.TimestampCompressionModel;
import compression.timestamp.TimestampCompressionModelType;
import compression.value.ValueCompressionModel;
import compression.value.ValueCompressionModelType;
import config.ConfigProperties;
import records.CompressionModel;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ModelPickerGreedy extends ModelPicker {

    private static final int lengthBound = ConfigProperties.getInstance().getModelLengthBound();
    private static final double gorillaBestBytePerDatapoint = getGorillaBestBytePerDatapoint();
    private static final double siDiffAndDeltaDeltaBestBytesPerDataPoint = getSiDiffDeltaDeltaBestBytesPerDatapoint();

    private static double getGorillaBestBytePerDatapoint() {
        int bitsForGorillaValueNotSame = 2 + 4 + 5 + 1;//lowest possible amount of bits for a gorilla value that is not the same as the rest

        // Store 1 float as an int e.g. 32 bits, + 1 bit per value (except two of them) + 1 value that is not the same as the others
        int bitsUsedForGorilla = Integer.SIZE + (lengthBound - 2) + bitsForGorillaValueNotSame;

        double bytesUsedByGorilla = Math.ceil(bitsUsedForGorilla / (double) Byte.SIZE);
        return (bytesUsedByGorilla + overheadPerModel) / lengthBound;
    }

    private static double getSiDiffDeltaDeltaBestBytesPerDatapoint() {
        int controlBits = 2;
        int signBits = 1;
        int smallestNonZeroBucketSizeInBits = BucketEncoding.getSmallestNonZeroBucketSizeInBits();
        int bitsForSmallestBucketSize = smallestNonZeroBucketSizeInBits + controlBits + signBits;
        // We have two values using the non-zero bucket (the SI/base-delta) and another non-zero value
        int bitsUsedByModel = 2 * bitsForSmallestBucketSize + (lengthBound - 2) * controlBits; //TODO if simon no fix signed encoder add signBit
        double bytesUsedByModel = Math.ceil(bitsUsedByModel / (double) Byte.SIZE);
        return (bytesUsedByModel + overheadPerModel) / lengthBound;
    }

    @Override
    public CompressionModel findBestCompressionModel(List<ValueCompressionModel> valueCompressionModels, List<TimestampCompressionModel> timestampCompressionModels) {
        List<ValueCompressionModel> valueModels = new ArrayList<>(valueCompressionModels);
        List<TimestampCompressionModel> timeStampModels = new ArrayList<>(timestampCompressionModels);

        ValueCompressionModel bestValueCompressionModel = getBestValueModel(valueModels);
        TimestampCompressionModel bestTimestampCompressionModel = getBestTimeStampModel(timeStampModels);

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
        //Check if it is allowed to ignore gorilla
        performShortCircuitingValueModels(valueCompressionModelsList);

        return valueCompressionModelsList.stream()
                .min(Comparator.comparing(this::calculateAmountBytesPerDataPoint))
                .orElseThrow(() -> new RuntimeException("In ModelPicker:getBestValueModel() - models list.empty should not happen"));
    }

    private void performShortCircuitingValueModels(List<ValueCompressionModel> valueCompressionModelsList) {
        Map<ValueCompressionModelType, ValueCompressionModel> typeToValueModel = valueCompressionModelsList.stream()
                .collect(Collectors.toMap(ValueCompressionModel::getValueCompressionModelType, item -> item));

        boolean gorillaRemoved = false;
        ValueCompressionModel pmcMeanModel = typeToValueModel.get(ValueCompressionModelType.PMC_MEAN);
        if (pmcMeanModel != null) {
            double pmcMeanBytesPerDataPoint = calculateAmountBytesPerDataPoint(pmcMeanModel);
            if (pmcMeanBytesPerDataPoint <= gorillaBestBytePerDatapoint) {
                valueCompressionModelsList.remove(typeToValueModel.get(ValueCompressionModelType.GORILLA));
                gorillaRemoved = true;
            }
        }
        ValueCompressionModel swingModel = typeToValueModel.get(ValueCompressionModelType.PMC_MEAN);
        if (swingModel != null && !gorillaRemoved) {
            double swingBytesPerDataPoint = calculateAmountBytesPerDataPoint(swingModel);
            if (swingBytesPerDataPoint <= gorillaBestBytePerDatapoint) {
                valueCompressionModelsList.remove(typeToValueModel.get(ValueCompressionModelType.GORILLA));
            }
        }
    }

    protected TimestampCompressionModel getBestTimeStampModel(List<TimestampCompressionModel> timestampCompressionModelsList) {
        //Can we ignore SIDiff or DeltaDelta
        performShortCircuitingTimestampModels(timestampCompressionModelsList);

        return timestampCompressionModelsList.stream()
                .min(Comparator.comparing(this::calculateAmountBytesPerDataPoint))
                .orElseThrow(() -> new RuntimeException("In ModelPicker:getBestTimeStampModel() - Should not happen"));
    }

    private void performShortCircuitingTimestampModels(List<TimestampCompressionModel> timestampCompressionModelsList) {
        Map<TimestampCompressionModelType, TimestampCompressionModel> typeToTimestampModel = timestampCompressionModelsList.stream()
                .collect(Collectors.toMap(TimestampCompressionModel::getTimestampCompressionModelType, item -> item));

        TimestampCompressionModel regularModel = typeToTimestampModel.get(TimestampCompressionModelType.REGULAR);

        if (regularModel != null) {
            double bytesPerDataPointRegular = calculateAmountBytesPerDataPoint(regularModel);
            if (bytesPerDataPointRegular < siDiffAndDeltaDeltaBestBytesPerDataPoint) {
                timestampCompressionModelsList.remove(typeToTimestampModel.get(TimestampCompressionModelType.SIDIFF));
                timestampCompressionModelsList.remove(typeToTimestampModel.get(TimestampCompressionModelType.DELTADELTA));
            }
        }
    }

}
