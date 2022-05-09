package compression.timestamp;

import compression.BlobDecompressor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import records.DataPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class SIDiffTimestampCompressionModelTest {
    Random random;

    // Helper that creates random data points in increasing order
    private DataPoint createDataPoint(){
        long previousLong = 0;
        float previousFloat = 0;

        previousLong += random.nextLong(100L);
        previousFloat += random.nextFloat(100.00f);
        return new DataPoint(previousLong, previousFloat);
    }

    private List<DataPoint> createTenDataPoints(){
        List<DataPoint> dataPointList = new ArrayList<>();
        for(int i = 0; i < 10; i++){
            dataPointList.add(createDataPoint());
        }
        return dataPointList;
    }


    @BeforeEach
    void beforeEach(){
        random = new Random();
    }

    @Test
    public void testModelAppendsTwice(){
        TimestampCompressionModel siDiffTimestampModelType = new SIDiffTimestampCompressionModel(0, Integer.MAX_VALUE);
        boolean append1 = siDiffTimestampModelType.append(createDataPoint());
        boolean append2 = siDiffTimestampModelType.append(createDataPoint());

        Assertions.assertTrue(append1);
        Assertions.assertTrue(append2);
    }

    @Test
    public void testModelResets(){
        TimestampCompressionModel siDiffTimestampModelType = new SIDiffTimestampCompressionModel(0, Integer.MAX_VALUE);
        siDiffTimestampModelType.append(createDataPoint());
        siDiffTimestampModelType.append(createDataPoint());
        siDiffTimestampModelType.append(createDataPoint());

        List<DataPoint> dataPointList = createTenDataPoints();
        boolean success = siDiffTimestampModelType.resetAndAppendAll(dataPointList);
        int actualLength = siDiffTimestampModelType.getLength();

        Assertions.assertTrue(success);
        Assertions.assertEquals(10, actualLength);
    }

    @Test
    public void testReduceSizeToN(){
        TimestampCompressionModel siDiffTimestampModelType = new SIDiffTimestampCompressionModel(0, Integer.MAX_VALUE);
        List<DataPoint> dataPoints = createTenDataPoints();
        dataPoints.forEach(siDiffTimestampModelType::append);
        siDiffTimestampModelType.reduceToSizeN(5);
        int actualAmountOfTimestamps = siDiffTimestampModelType.getLength();

        // Assert list has 5 data points
        assertEquals(5, actualAmountOfTimestamps);
    }

    @Test
    public void testReduceSizeWithNumberOfTimestamps(){
        TimestampCompressionModel siDiffTimestampModelType = new SIDiffTimestampCompressionModel(0, Integer.MAX_VALUE);
        List<DataPoint> dataPoints = createTenDataPoints();
        dataPoints.forEach(siDiffTimestampModelType::append);
        int expectedAmountTimestamps = dataPoints.size();
        siDiffTimestampModelType.reduceToSizeN(expectedAmountTimestamps);

        assertEquals(expectedAmountTimestamps, siDiffTimestampModelType.getLength());
    }

    @Test
    public void testReduceSizeWithZeroThrowsException(){
        TimestampCompressionModel siDiffTimestampModelType = new SIDiffTimestampCompressionModel(0, Integer.MAX_VALUE);
        List<DataPoint> dataPoints = createTenDataPoints();
        dataPoints.forEach(siDiffTimestampModelType::append);
        assertThrows(IllegalArgumentException.class, () -> siDiffTimestampModelType.reduceToSizeN(0));
    }

    @Test
    public void testReduceSizeWithMoreThanListSizeThrowsException(){
        TimestampCompressionModel siDiffTimestampModelType = new SIDiffTimestampCompressionModel(0, Integer.MAX_VALUE);
        List<DataPoint> dataPoints = createTenDataPoints();
        dataPoints.forEach(siDiffTimestampModelType::append);
        assertThrows(IllegalArgumentException.class, () -> siDiffTimestampModelType.reduceToSizeN(20));
    }

    @Test
    public void testBlobRepresentation1(){
        TimestampCompressionModel siDiffTimestampModelType = new SIDiffTimestampCompressionModel(0, Integer.MAX_VALUE);
        List<DataPoint> dataPoints = createTenDataPoints();

        dataPoints.forEach(siDiffTimestampModelType::append);

        var blobRepresentation = siDiffTimestampModelType.getBlobRepresentation();

        var decodedValues = BlobDecompressor.decompressTimestamps(TimestampCompressionModelType.SIDIFF,
                blobRepresentation, dataPoints.get(0).timestamp(), dataPoints.get(dataPoints.size() - 1).timestamp());

        for (int i = 0; i < decodedValues.size(); i++){
            Assertions.assertEquals(dataPoints.get(i).timestamp(), decodedValues.get(i));
        }
    }


    @Test
    void testProblematicNegativeDifferences() {
        int threshold = 50;
        TimestampCompressionModel siDiffTimestampModelType = new SIDiffTimestampCompressionModel(threshold, Integer.MAX_VALUE);
        List<Long> timestamps = List.of(0L, 200L, 245L, 750L, 1000L);
        List<DataPoint> dataPoints = createDataPointsFromTimestamps(timestamps);
        boolean success = siDiffTimestampModelType.resetAndAppendAll(dataPoints);
        Assertions.assertTrue(success);

        List<Long> decompressedTimestamps = BlobDecompressor.decompressTimestamps(siDiffTimestampModelType.getTimestampCompressionModelType(),
                siDiffTimestampModelType.getBlobRepresentation(),
                timestamps.get(0),
                timestamps.get(timestamps.size() - 1)
        );


        for (int i = 0; i < decompressedTimestamps.size(); i++) {
            if (i != 0) {
                // Current timestamp should be larger than previous
                Assertions.assertTrue(decompressedTimestamps.get(i) > decompressedTimestamps.get(i -1));
            }
            long diff = timestamps.get(i) - decompressedTimestamps.get(i);
            Assertions.assertTrue(diff < threshold);
        }
    }

    @Test
    void testProblematicNegativeDifferences2() {
        int threshold = 50;
        TimestampCompressionModel siDiffTimestampModelType = new SIDiffTimestampCompressionModel(threshold, Integer.MAX_VALUE);
        List<Long> timestamps = List.of(0L, 50L, 75L, 300L);
        List<DataPoint> dataPoints = createDataPointsFromTimestamps(timestamps);
        boolean success = siDiffTimestampModelType.resetAndAppendAll(dataPoints);
        Assertions.assertTrue(success);

        List<Long> decompressedTimestamps = BlobDecompressor.decompressTimestamps(siDiffTimestampModelType.getTimestampCompressionModelType(),
                siDiffTimestampModelType.getBlobRepresentation(),
                timestamps.get(0),
                timestamps.get(timestamps.size() - 1)
        );


        for (int i = 0; i < decompressedTimestamps.size(); i++) {
            if (i != 0) {
                // Current timestamp should be larger than previous
                Assertions.assertTrue(decompressedTimestamps.get(i) > decompressedTimestamps.get(i -1));
            }
            long diff = timestamps.get(i) - decompressedTimestamps.get(i);
            Assertions.assertTrue(diff < threshold);
        }
    }


    @Test
    void testProblematicPositiveDifferences() {
        int threshold = 50;
        TimestampCompressionModel siDiffTimestampModelType = new SIDiffTimestampCompressionModel(threshold, Integer.MAX_VALUE);
        List<Long> timestamps = List.of(0L, 545L, 550L, 750L, 1000L);
        List<DataPoint> dataPoints = createDataPointsFromTimestamps(timestamps);
        boolean success = siDiffTimestampModelType.resetAndAppendAll(dataPoints);
        Assertions.assertTrue(success);

        List<Long> decompressedTimestamps = BlobDecompressor.decompressTimestamps(siDiffTimestampModelType.getTimestampCompressionModelType(),
                siDiffTimestampModelType.getBlobRepresentation(),
                timestamps.get(0),
                timestamps.get(timestamps.size() - 1)
        );


        for (int i = 0; i < decompressedTimestamps.size(); i++) {
            if (i != 0) {
                // Current timestamp should be larger than previous
                Assertions.assertTrue(decompressedTimestamps.get(i) > decompressedTimestamps.get(i -1));
            }
            Assertions.assertTrue(dataPoints.get(i).timestamp() - decompressedTimestamps.get(i) < threshold);
        }
    }

    private List<DataPoint> createDataPointsFromTimestamps(List<Long> timestamps) {
        List<DataPoint> dataPoints = new ArrayList<>();
        for (Long timestamp : timestamps) {
            dataPoints.add(createDataPointForTimestamp(timestamp));
        }
        return dataPoints;
    }

    private DataPoint createDataPointForTimestamp(long timeStamp) {
        // We use -1 for our data points as value because this model does not care about the values of the data points
        return new DataPoint(timeStamp, -1);
    }
}