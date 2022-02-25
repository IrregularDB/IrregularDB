package compression;

import compression.encoding.BucketEncoding;
import compression.timestamp.TimeStampCompressionModelType;
import compression.utility.BitBuffer;
import compression.utility.BitStream;
import compression.value.ValueCompressionModelType;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class BlobDecompressor {

    public static  List<DataPoint> decompressBlobs(TimeStampCompressionModelType timeStampModelType, ByteBuffer timeStampBlob,
                                           ValueCompressionModelType valueModelType, ByteBuffer valueBlob,
                                           Long startTime, Long endTime) {
        List<Long> timeStamps = decompressTimeStamps(timeStampModelType, timeStampBlob, startTime, endTime);
        return createDataPointsByDecompressingValues(valueModelType, valueBlob, timeStamps);
    }

    static List<Long> decompressTimeStamps(TimeStampCompressionModelType timeStampModelType, ByteBuffer timeStampBlob,
                                            Long startTime, Long endTime) {
        return switch (timeStampModelType) {
            case REGULAR -> decompressRegular(timeStampBlob, startTime, endTime);
            case DELTAPAIRS -> decompressDeltaPairs(timeStampBlob, startTime);
            case BASEDELTA -> decompressBaseDelta(timeStampBlob, startTime);
            case RECOMPUTESI -> decompressRecomputeSI(timeStampBlob);
            default -> throw new IllegalArgumentException("No decompression method has been implemented for the given Time Stamp Model Type");
        };
    }

    private static List<Long> decompressRegular(ByteBuffer timeStampBlob, Long startTime, Long endTime) {
        int si = timeStampBlob.getInt(0);
        long currTime = startTime;

        List<Long> timeStamps = new ArrayList<>();
        while (currTime <= endTime) {
            timeStamps.add(currTime);
            currTime += si;
        }
        return timeStamps;
    }

    private static List<Long> decompressDeltaPairs(ByteBuffer timeStampBlob, long startTime) {
        BitStream bitStream = new BitStream(timeStampBlob);
        List<Integer> deltaPairsTimeStamps = BucketEncoding.decode(bitStream);

        List<Long> timeStamps = new ArrayList<>();
        timeStamps.add(startTime);

        long prevValue = startTime;
        for (Integer deltaPairsTimeStamp : deltaPairsTimeStamps) {
            prevValue += deltaPairsTimeStamp;
            timeStamps.add(prevValue);
        }
        return timeStamps;
    }

    private static List<Long> decompressBaseDelta(ByteBuffer timeStampBlob, Long startTime) {
        LinkedList<Integer> deltaTimeStamps = new LinkedList<>();
        int index = 0;

        while(index < timeStampBlob.limit()){
            deltaTimeStamps.addLast(timeStampBlob.getInt(index));
            index += Integer.BYTES;
            timeStampBlob.position(index);
        }

        List<Long> timeStamps = deltaTimeStamps.stream()
                .map(delta -> startTime + delta)
                .collect(Collectors.toList());

        timeStamps.add(0, startTime);

        return timeStamps;
    }

    private static List<Long> decompressRecomputeSI(ByteBuffer timeStampBlob) {
        throw new RuntimeException("Not implemented");
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
        throw new RuntimeException("Not implemented");
    }
}
