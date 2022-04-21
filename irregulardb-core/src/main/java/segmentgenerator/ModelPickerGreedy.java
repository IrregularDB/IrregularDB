package segmentgenerator;

import compression.encoding.BucketEncoding;
import compression.timestamp.TimestampCompressionModel;
import compression.timestamp.TimestampCompressionModelType;
import compression.value.ValueCompressionModel;
import compression.value.ValueCompressionModelType;
import config.ConfigProperties;
import records.CompressionModel;
import records.Pair;

import java.util.*;
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
        int bitsUsedByModel = 2 * bitsForSmallestBucketSize + (lengthBound - 2) * controlBits;
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
            bestTimestampCompressionModel.reduceToSizeN(bestValueCompressionModel.getLength());
        } else {
            bestValueCompressionModel.reduceToSizeN(bestTimestampCompressionModel.getLength());
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

        Pair<Double, ValueCompressionModel> bestModel = new Pair<>(Double.MAX_VALUE, null);
        for (ValueCompressionModel valueCompressionModel : valueCompressionModelsList) {
            double bytesPerPoint = calculateAmountBytesPerDataPoint(valueCompressionModel);
            if (bytesPerPoint < bestModel.f0()) {
                bestModel = new Pair<>(bytesPerPoint, valueCompressionModel);
            }
        }
        return bestModel.f1();
    }

    private void performShortCircuitingValueModels(List<ValueCompressionModel> valueCompressionModelsList) {
        Map<ValueCompressionModelType, ValueCompressionModel> typeToValueModel = new HashMap<>();
        for (ValueCompressionModel valueCompressionModel : valueCompressionModelsList) {
            typeToValueModel.put(valueCompressionModel.getValueCompressionModelType(), valueCompressionModel);
        }

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

        Pair<Double, TimestampCompressionModel> bestModel = new Pair<>(Double.MAX_VALUE, null);
        for (TimestampCompressionModel timestampCompressionModel : timestampCompressionModelsList) {
            double bytesPerPoint = calculateAmountBytesPerDataPoint(timestampCompressionModel);
            if (bytesPerPoint < bestModel.f0()) {
                bestModel = new Pair<>(bytesPerPoint, timestampCompressionModel);
            }
        }
        return bestModel.f1();
    }

    private void performShortCircuitingTimestampModels(List<TimestampCompressionModel> timestampCompressionModelsList) {
        Map<TimestampCompressionModelType, TimestampCompressionModel> typeToTimestampModel = new HashMap<>();
        for (TimestampCompressionModel timestampCompressionModel : timestampCompressionModelsList) {
            typeToTimestampModel.put(timestampCompressionModel.getTimestampCompressionModelType(), timestampCompressionModel);
        }

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
