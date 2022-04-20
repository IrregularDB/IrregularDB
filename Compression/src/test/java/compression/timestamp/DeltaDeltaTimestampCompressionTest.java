package compression.timestamp;

import compression.BlobDecompressor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import records.DataPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class DeltaDeltaTimestampCompressionTest {

    Random random = new Random();
    long previousLong;
    float previousFloat;
    List<DataPoint> dataPoints = createTenDataPoints();
    private DeltaDeltaTimestampCompressionModel deltaDeltaTimestampCompressionModel;

    @BeforeEach
    void beforeEach(){
        deltaDeltaTimestampCompressionModel = new DeltaDeltaTimestampCompressionModel(0, Integer.MAX_VALUE);
    }

    @Test
    public void testModelAppendsTwice(){
        boolean append1 = deltaDeltaTimestampCompressionModel.append(createDataPoint());
        boolean append2 = deltaDeltaTimestampCompressionModel.append(createDataPoint());

        Assertions.assertTrue(append1);
        Assertions.assertTrue(append2);
    }

    @Test
    public void testModelResets(){
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
    public void testReduceSizeToN(){
        dataPoints.forEach(dp -> deltaDeltaTimestampCompressionModel.append(dp));
        deltaDeltaTimestampCompressionModel.reduceToSizeN(5);
        int actualAmountOfTimestamps = deltaDeltaTimestampCompressionModel.getLength();

        // Assert list has 5 data points
        Assertions.assertEquals(5, actualAmountOfTimestamps);
    }

    @Test
    public void testReduceSizeWithNumberOfTimestamps(){
        dataPoints.forEach(dp -> deltaDeltaTimestampCompressionModel.append(dp));
        int expectedAmountTimestamps = dataPoints.size();
        deltaDeltaTimestampCompressionModel.reduceToSizeN(expectedAmountTimestamps);

        Assertions.assertEquals(expectedAmountTimestamps, deltaDeltaTimestampCompressionModel.getLength());
    }

    @Test
    public void testReduceSizeWithZeroThrowsException(){
        dataPoints.forEach(dp -> deltaDeltaTimestampCompressionModel.append(dp));
        Assertions.assertThrows(IllegalArgumentException.class, () -> deltaDeltaTimestampCompressionModel.reduceToSizeN(0));
    }

    @Test
    public void testReduceSizeWithMoreThanListSizeThrowsException(){
        dataPoints.forEach(dp -> deltaDeltaTimestampCompressionModel.append(dp));
        Assertions.assertThrows(IllegalArgumentException.class, () -> deltaDeltaTimestampCompressionModel.reduceToSizeN(20));
    }

    @Test
    public void testPushedWithinThreshold(){
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
    public void testPushedDeltaDeltas(){
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
                new DataPoint(146, -1), //will be pushed to 150
                new DataPoint(720, -1), //= pre value(150) + prev delta(50) + bucketMaxAndThres(520), will be pushed to 150 + 50 + 511
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
    public void test1DataPoint() {
        //expected max bucket size 0, 511, 65535, max int
        final int threshold = 10;
        List<DataPoint> dataPoints = List.of(
                new DataPoint(50, -1)
        );

        deltaDeltaTimestampCompressionModel = new DeltaDeltaTimestampCompressionModel(threshold, Integer.MAX_VALUE);
        deltaDeltaTimestampCompressionModel.resetAndAppendAll(dataPoints);

        List<Long> decompressedTimestamps = BlobDecompressor.decompressTimestamps(TimestampCompressionModelType.DELTADELTA,
                deltaDeltaTimestampCompressionModel.getBlobRepresentation(),
                dataPoints.get(0).timestamp(),
                dataPoints.get(dataPoints.size() - 1).timestamp()
        );

        Assertions.assertEquals(50, decompressedTimestamps.get(0));
    }


    // Helper that creates random data points in increasing order
    private DataPoint createDataPoint(){
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

}