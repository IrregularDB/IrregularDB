package compression;

import compression.encoding.GorillaValueEncoding;
import compression.encoding.SignedBucketEncoder;
import compression.encoding.SingleIntEncoding;
import compression.timestamp.TimestampCompressionModelType;
import compression.utility.BitStream.BitStream;
import compression.utility.BitStream.BitStreamNew;
import compression.value.ValueCompressionModelType;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class BlobDecompressor {

    public static  List<DataPoint> decompressBlobs(TimestampCompressionModelType timeStampModelType, ByteBuffer timeStampBlob,
                                                   ValueCompressionModelType valueModelType, ByteBuffer valueBlob,
                                                   Long startTime, Long endTime) {
        List<Long> timeStamps = decompressTimeStamps(timeStampModelType, timeStampBlob, startTime, endTime);
        return createDataPointsByDecompressingValues(valueModelType, valueBlob, timeStamps);
    }

    public static List<Long> decompressTimeStamps(TimestampCompressionModelType timeStampModelType, ByteBuffer timeStampBlob,
                                                  Long startTime, Long endTime) {
        return switch (timeStampModelType) {
            case REGULAR -> decompressRegular(timeStampBlob, startTime, endTime);
            case DELTADELTA -> decompressDeltaDelta(timeStampBlob, startTime);
            default -> throw new IllegalArgumentException("No decompression method has been implemented for the given Time Stamp Model Type");
        };
    }

    private static List<Long> decompressRegular(ByteBuffer timeStampBlob, Long startTime, Long endTime) {
        int si = SingleIntEncoding.decode(timeStampBlob);
        long currTime = startTime;

        List<Long> timeStamps = new ArrayList<>();
        while (currTime <= endTime) {
            timeStamps.add(currTime);
            currTime += si;
        }
        return timeStamps;
    }

    private static List<Long> decompressDeltaDelta(ByteBuffer timestampBlob, long startTime){
        BitStream bitStream = new BitStreamNew(timestampBlob);
        SignedBucketEncoder signedBucketEncoder = new SignedBucketEncoder();
        List<Integer> deltaDeltaTimes = signedBucketEncoder.decodeSigned(bitStream);

        // First value in deltaDelta encoding is a delta value from start time
        Integer previousDelta = deltaDeltaTimes.get(0);
        long previousTimestamp = startTime + previousDelta;

        List<Long> originalTimestamps = new ArrayList<>();

        // Add start time
        originalTimestamps.add(startTime);

        // Add second time stamp as delta from start time and remove it
        originalTimestamps.add(startTime + previousDelta);
        deltaDeltaTimes.remove(0);

        for (Integer deltaDelta : deltaDeltaTimes){
            previousTimestamp += previousDelta + deltaDelta;
            previousDelta += deltaDelta;

            originalTimestamps.add(previousTimestamp);
        }

        return originalTimestamps;
    }

    static List<DataPoint> createDataPointsByDecompressingValues(ValueCompressionModelType valueModelType, ByteBuffer valueBlob, List<Long> timeStamps) {
        return switch (valueModelType) {
            case PMC_MEAN -> decompressPMCMean(valueBlob, timeStamps);
            case SWING -> decompressSwing(valueBlob, timeStamps);
            case GORILLA -> decompressGorilla(valueBlob, timeStamps);
            default -> throw new IllegalArgumentException("No decompression method has been implemented for the given Value Model Type");
        };
    }

    private static List<DataPoint> decompressPMCMean(ByteBuffer valueBlob, List<Long> timeStamps) {
        float meanValue = valueBlob.getFloat(0);
        return timeStamps.stream().map(timeStamp -> new DataPoint(timeStamp, meanValue)).toList();
    }

    private static List<DataPoint> decompressSwing(ByteBuffer valueBlob, List<Long> timeStamps) {
        final float slope = valueBlob.getFloat(0);
        final float intercept = valueBlob.getFloat(4);

        // TODO: consider just using a linear function object
        Function<Long, Float> linearFunction = (Long time) -> time * slope + intercept;

        return timeStamps.stream().map(timeStamp -> new DataPoint(timeStamp, linearFunction.apply(timeStamp))).toList();
    }

    private static List<DataPoint> decompressGorilla(ByteBuffer valueBlob, List<Long> timeStamps) {
        BitStream bitStream = new BitStreamNew(valueBlob);

        List<Float> decodedValues = GorillaValueEncoding.decode(bitStream);
        int amtValues = decodedValues.size();
        if (amtValues != timeStamps.size()) {
            throw new RuntimeException("The amount of values and time stamps did not match up");
        }
        List<DataPoint> dataPoints = new ArrayList<>();
        for (int i = 0; i < amtValues; i++) {
            dataPoints.add(new DataPoint(timeStamps.get(i), decodedValues.get(i)));
        }
        return dataPoints;
    }
}
