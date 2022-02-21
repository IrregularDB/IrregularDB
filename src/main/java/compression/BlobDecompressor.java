package compression;

import compression.timestamp.TimeStampCompressionModelType;
import compression.utility.LinearFunction;
import compression.value.ValueCompressionModelType;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class BlobDecompressor {

    public static  List<DataPoint> decompressBlobs(TimeStampCompressionModelType timeStampModelType, ByteBuffer timeStampBlob,
                                           ValueCompressionModelType valueModelType, ByteBuffer valueBlob,
                                           Long startTime, Long endTime) {
        List<Long> timeStamps = decompressTimeStamps(timeStampModelType, timeStampBlob, startTime, endTime);
        return decompressValuesAndCreateDataPoints(valueModelType, valueBlob, timeStamps);
    }

    static List<Long> decompressTimeStamps(TimeStampCompressionModelType timeStampModelType, ByteBuffer timeStampBlob,
                                            Long startTime, Long endTime) {
        return switch (timeStampModelType) {
            case REGULAR -> decompressRegular(timeStampBlob, startTime, endTime);
            case DELTAPAIRS -> decompressDeltaPairs(timeStampBlob);
            case BASEDELTA -> decompressBaseDelta(timeStampBlob);
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

    private static List<Long> decompressDeltaPairs(ByteBuffer timeStampBlob) {
        throw new RuntimeException("Not implemented");
    }

    private static List<Long> decompressBaseDelta(ByteBuffer timeStampBlob) {
        throw new RuntimeException("Not implemented");
    }

    private static List<Long> decompressRecomputeSI(ByteBuffer timeStampBlob) {
        throw new RuntimeException("Not implemented");
    }

    static List<DataPoint> decompressValuesAndCreateDataPoints(ValueCompressionModelType valueModelType, ByteBuffer valueBlob, List<Long> timeStamps) {
        return switch (valueModelType) {
            case PMCMEAN -> decompressPMCMean(valueBlob, timeStamps);
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
        float slope = valueBlob.getFloat(0);
        float intercept = valueBlob.getFloat(4);

        return timeStamps.stream().map(timeStamp -> new DataPoint(timeStamp, getValue(slope, intercept, timeStamp))).toList();
    }

    private static double getValue(float slope, float intercept, Long timeStamp) {
        return slope * timeStamp + intercept;
    }

    private static List<DataPoint> decompressGorilla(ByteBuffer valueBlob, List<Long> timeStamps) {
        throw new RuntimeException("Not implemented");
    }
}