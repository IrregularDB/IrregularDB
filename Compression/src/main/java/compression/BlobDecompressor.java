package compression;

import compression.encoding.BucketEncoding;
import compression.encoding.GorillaValueEncoding;
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

    public static  List<DataPoint> decompressBlobs(TimestampCompressionModelType timestampModelType, ByteBuffer timestampBlob,
                                                   ValueCompressionModelType valueModelType, ByteBuffer valueBlob,
                                                   Long startTime, Long endTime) {
        List<Long> timestamps = decompressTimestamps(timestampModelType, timestampBlob, startTime, endTime);
        return createDataPointsByDecompressingValues(valueModelType, valueBlob, timestamps);
    }

    public static List<Long> decompressTimestamps(TimestampCompressionModelType timestampModelType, ByteBuffer timestampBlob,
                                                  Long startTime, Long endTime) {
        return switch (timestampModelType) {
            case REGULAR -> decompressRegular(timestampBlob, startTime, endTime);
            case DELTADELTA -> decompressDeltaDelta(timestampBlob, startTime);
            case SIDIFF -> decompressSIDiff(timestampBlob, startTime);
            default -> throw new IllegalArgumentException("No decompression method has been implemented for the given Time Stamp Model Type");
        };
    }

    private static List<Long> decompressRegular(ByteBuffer timestampBlob, Long startTime, Long endTime) {
        int si = SingleIntEncoding.decode(timestampBlob);
        long currTime = startTime;

        List<Long> timestamps = new ArrayList<>();
        while (currTime <= endTime) {
            timestamps.add(currTime);
            currTime += si;
        }
        return timestamps;
    }

    private static List<Long> decompressDeltaDelta(ByteBuffer timestampBlob, long startTime){
        if (timestampBlob.limit() == 0) {
            return List.of(startTime);
        }
        BitStream bitStream = new BitStreamNew(timestampBlob);

        List<Integer> deltaDeltaTimes = BucketEncoding.decode(bitStream, true);

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

    private static List<Long> decompressSIDiff(ByteBuffer timestampBlob, long startTime){
        List<Long> timestamps = new ArrayList<>();
        timestamps.add(startTime);

        BitStream bitStream = new BitStreamNew(timestampBlob);
        List<Integer> decodedValues = BucketEncoding.decode(bitStream, true);

        int si = decodedValues.get(0);
        long expectedTimestamp = startTime + si;
        for (int i = 1; i < decodedValues.size(); i++) {
            int difference = decodedValues.get(i);
            timestamps.add(expectedTimestamp + difference);
            expectedTimestamp += si;
        }

        return timestamps;
    }

    static List<DataPoint> createDataPointsByDecompressingValues(ValueCompressionModelType valueModelType, ByteBuffer valueBlob, List<Long> timestamps) {
        return switch (valueModelType) {
            case PMC_MEAN -> decompressPMCMean(valueBlob, timestamps);
            case SWING -> decompressSwing(valueBlob, timestamps);
            case GORILLA -> decompressGorilla(valueBlob, timestamps);
            default -> throw new IllegalArgumentException("No decompression method has been implemented for the given Value Model Type");
        };
    }

    private static List<DataPoint> decompressPMCMean(ByteBuffer valueBlob, List<Long> timestamps) {
        float meanValue = valueBlob.getFloat(0);
        return timestamps.stream().map(timeStamp -> new DataPoint(timeStamp, meanValue)).toList();
    }

    private static List<DataPoint> decompressSwing(ByteBuffer valueBlob, List<Long> timestamps) {
        Long startTime = timestamps.get(0);
        final float slope = valueBlob.getFloat(0);
        final float intercept = valueBlob.getFloat(4);

        // TODO: consider just using a linear function object
        Function<Long, Float> linearFunction = (Long time) -> time * slope + intercept;

        return timestamps.stream().map(timeStamp -> new DataPoint(timeStamp, linearFunction.apply(timeStamp - startTime))).toList();
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
