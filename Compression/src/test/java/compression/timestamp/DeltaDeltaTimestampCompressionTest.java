package compression.timestamp;

import compression.BlobDecompressor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import records.DataPoint;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

class DeltaDeltaTimestampCompressionTest {
    Random random = new Random();
    long previousLong;
    float previousFloat;
    private DeltaDeltaTimestampCompressionModel deltaDeltaTimestampCompressionModel;

    @BeforeEach
    void beforeEach() {
        deltaDeltaTimestampCompressionModel = new DeltaDeltaTimestampCompressionModel(0, Integer.MAX_VALUE);
    }

    @Test
    public void testModelAppendsTwice() {
        boolean append1 = deltaDeltaTimestampCompressionModel.append(createDataPoint());
        boolean append2 = deltaDeltaTimestampCompressionModel.append(createDataPoint());

        Assertions.assertTrue(append1);
        Assertions.assertTrue(append2);
    }

    @Test
    public void testModelResets() {
        deltaDeltaTimestampCompressionModel.append(createDataPoint());
        deltaDeltaTimestampCompressionModel.append(createDataPoint());
        deltaDeltaTimestampCompressionModel.append(createDataPoint());

        List<DataPoint> dataPointList = createTenDataPoints();
        boolean success = deltaDeltaTimestampCompressionModel.resetAndAppendAll(dataPointList);
        int actualLength = deltaDeltaTimestampCompressionModel.getLength();

        Assertions.assertTrue(success);
        Assertions.assertEquals(10, actualLength);
    }

    @Test
    public void testReduceSizeToN() {
        List<DataPoint> dataPoints = createTenDataPoints();
        dataPoints.forEach(dp -> deltaDeltaTimestampCompressionModel.append(dp));
        deltaDeltaTimestampCompressionModel.reduceToSizeN(5);
        int actualAmountOfTimestamps = deltaDeltaTimestampCompressionModel.getLength();

        // Assert list has 5 data points
        Assertions.assertEquals(5, actualAmountOfTimestamps);
    }

    @Test
    public void testReduceSizeWithNumberOfTimestamps() {
        List<DataPoint> dataPoints = createTenDataPoints();
        dataPoints.forEach(dp -> deltaDeltaTimestampCompressionModel.append(dp));
        int expectedAmountTimestamps = dataPoints.size();
        deltaDeltaTimestampCompressionModel.reduceToSizeN(expectedAmountTimestamps);

        Assertions.assertEquals(expectedAmountTimestamps, deltaDeltaTimestampCompressionModel.getLength());
    }

    @Test
    public void testReduceSizeWithZeroThrowsException() {
        List<DataPoint> dataPoints = createTenDataPoints();
        dataPoints.forEach(dp -> deltaDeltaTimestampCompressionModel.append(dp));
        Assertions.assertThrows(IllegalArgumentException.class, () -> deltaDeltaTimestampCompressionModel.reduceToSizeN(0));
    }

    @Test
    public void testReduceSizeWithMoreThanListSizeThrowsException() {
        List<DataPoint> dataPoints = createTenDataPoints();
        dataPoints.forEach(dp -> deltaDeltaTimestampCompressionModel.append(dp));
        Assertions.assertThrows(IllegalArgumentException.class, () -> deltaDeltaTimestampCompressionModel.reduceToSizeN(20));
    }

    @Test
    public void testPushedWithinThreshold() {
        final int threshold = 10;
        List<DataPoint> dataPoints = List.of(
                new DataPoint(50, -1),
                new DataPoint(100, -1),
                new DataPoint(146, -1),
                new DataPoint(208, -1),
                new DataPoint(242, -1),
                new DataPoint(330, -1)
        );

        deltaDeltaTimestampCompressionModel = new DeltaDeltaTimestampCompressionModel(threshold, Integer.MAX_VALUE);
        deltaDeltaTimestampCompressionModel.resetAndAppendAll(dataPoints);

        List<Long> decompressedTimestamps = BlobDecompressor.decompressTimestamps(TimestampCompressionModelType.DELTADELTA,
                deltaDeltaTimestampCompressionModel.getBlobRepresentation(),
                dataPoints.get(0).timestamp(),
                dataPoints.get(dataPoints.size() - 1).timestamp()
        );

        //assert that timestamps within threshold
        for (int i = 0; i < dataPoints.size(); i++) {
            Assertions.assertTrue(Math.abs(dataPoints.get(i).timestamp() - decompressedTimestamps.get(i)) <= threshold);
        }

    }

    @Test
    public void testPushedDeltaDeltas() {
        final int threshold = 10;
        List<DataPoint> dataPoints = List.of(
                new DataPoint(50, -1),
                new DataPoint(100, -1),
                new DataPoint(146, -1),
                new DataPoint(202, -1),
                new DataPoint(242, -1),
                new DataPoint(330, -1)
        );

        deltaDeltaTimestampCompressionModel = new DeltaDeltaTimestampCompressionModel(threshold, Integer.MAX_VALUE);
        deltaDeltaTimestampCompressionModel.resetAndAppendAll(dataPoints);

        List<Long> decompressedTimestamps = BlobDecompressor.decompressTimestamps(TimestampCompressionModelType.DELTADELTA,
                deltaDeltaTimestampCompressionModel.getBlobRepresentation(),
                dataPoints.get(0).timestamp(),
                dataPoints.get(dataPoints.size() - 1).timestamp()
        );

        Assertions.assertEquals(50, decompressedTimestamps.get(0));
        Assertions.assertEquals(100, decompressedTimestamps.get(1));
        Assertions.assertEquals(150, decompressedTimestamps.get(2));
        Assertions.assertEquals(200, decompressedTimestamps.get(3));
        Assertions.assertEquals(250, decompressedTimestamps.get(4));
        Assertions.assertEquals(330, decompressedTimestamps.get(5));
    }

    @Test
    public void testPushedDeltaDeltasLargeBuckets() {
        //expected max bucket size 0, 511, 65535, max int
        final int threshold = 10;
        List<DataPoint> dataPoints = List.of(
                new DataPoint(50, -1),
                new DataPoint(100, -1),
                new DataPoint(155, -1), //will be pushed to 150 as DELTA = 50 works
                new DataPoint(720, -1), // curr delta = 720-150 = 570, = prev delta = 50. Delta-of-delta = 520, which will be pushed down to 511 giving 150 + (50 + 511) = 711
                new DataPoint(66812, -1)// deltadelta = 65535 + 5// prevValue(711) + prevDelta(711 - 150) + bucketMax(65535) + 5
        );

        deltaDeltaTimestampCompressionModel = new DeltaDeltaTimestampCompressionModel(threshold, Integer.MAX_VALUE);
        deltaDeltaTimestampCompressionModel.resetAndAppendAll(dataPoints);

        List<Long> decompressedTimestamps = BlobDecompressor.decompressTimestamps(TimestampCompressionModelType.DELTADELTA,
                deltaDeltaTimestampCompressionModel.getBlobRepresentation(),
                dataPoints.get(0).timestamp(),
                dataPoints.get(dataPoints.size() - 1).timestamp()
        );

        Assertions.assertEquals(50, decompressedTimestamps.get(0));
        Assertions.assertEquals(100, decompressedTimestamps.get(1));
        Assertions.assertEquals(150, decompressedTimestamps.get(2));
        Assertions.assertEquals(711, decompressedTimestamps.get(3));
        Assertions.assertEquals(66807, decompressedTimestamps.get(4));
    }

    @Test
    void testingToLargeDifferenceInTimeStamps() {
        List<Long> timestamps = Arrays.asList(1303382821000L, 1303382822000L, 1303382823000L, 1306097664000L, 1306097665000L);

        boolean success = deltaDeltaTimestampCompressionModel.resetAndAppendAll(createDataPointsFromTimestamps(timestamps));
        Assertions.assertFalse(success);
        // We expect to be able to handle the first 3 data points as they have a difference of 1000 between them
        // so they can be fitted. Then for the 3rd and 4th point we get:
        //   PREVIOUS DELTA: 1000
        //   DELTA-OF-DELTA: 2714840000 (which is larger than INT-max)
        Assertions.assertEquals(3, deltaDeltaTimestampCompressionModel.getLength());
    }

    @Test
    void testingToLargeDifferenceForInitialDelta() {
        long t1 = 1303382823000L;
        long t2 = t1 + 2L * Integer.MAX_VALUE;
        List<Long> timestamps = Arrays.asList(t1, t2);

        boolean success = deltaDeltaTimestampCompressionModel.resetAndAppendAll(createDataPointsFromTimestamps(timestamps));
        Assertions.assertFalse(success);
        // We expect to be able to handle only the first data point as the DELTA value between these two timestamps is
        // larger than INT_MAX
        Assertions.assertEquals(1, deltaDeltaTimestampCompressionModel.getLength());
        // DeltaDelta no longer supports models of size 1
        Assertions.assertFalse(deltaDeltaTimestampCompressionModel.canCreateByteBuffer());
    }

    @Test
    void testDeltaDeltaDoesNotBreakOrdering() {
        int threshold = 100;
        deltaDeltaTimestampCompressionModel = new DeltaDeltaTimestampCompressionModel(threshold, Integer.MAX_VALUE);
        List<Long> timestamps = List.of(0L, 200L, 300L, 350L);
        List<DataPoint> dataPoints = createDataPointsFromTimestamps(timestamps);
        boolean success = deltaDeltaTimestampCompressionModel.resetAndAppendAll(dataPoints);
        Assertions.assertTrue(success);

        List<Long> decompressedTimestamps = BlobDecompressor.decompressTimestamps(TimestampCompressionModelType.DELTADELTA,
                deltaDeltaTimestampCompressionModel.getBlobRepresentation(),
                dataPoints.get(0).timestamp(),
                dataPoints.get(dataPoints.size() - 1).timestamp()
        );


        for (int i = 0; i < decompressedTimestamps.size(); i++) {
            if (i != 0) {
                // Current timestamp should be larger than previous
                Assertions.assertTrue(decompressedTimestamps.get(i) > decompressedTimestamps.get(i -1));
            }
            Assertions.assertTrue(dataPoints.get(i).timestamp() - decompressedTimestamps.get(i) < threshold);
        }
    }

    // Helper that creates random data points in increasing order
    private DataPoint createDataPoint() {
        previousLong += random.nextLong(100L);
        previousFloat += random.nextFloat(100.00f);
        return new DataPoint(previousLong, previousFloat);
    }

    private List<DataPoint> createTenDataPoints() {
        List<DataPoint> dataPointList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            dataPointList.add(createDataPoint());
        }
        return dataPointList;
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