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
    private TimestampCompressionModel siDiffTimestampModelType;


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
        siDiffTimestampModelType = new SIDiffTimestampCompressionModel(0, Integer.MAX_VALUE);
        random = new Random();
    }

    @Test
    public void testModelAppendsTwice(){
        boolean append1 = siDiffTimestampModelType.append(createDataPoint());
        boolean append2 = siDiffTimestampModelType.append(createDataPoint());

        Assertions.assertTrue(append1);
        Assertions.assertTrue(append2);
    }

    @Test
    public void testModelResets(){
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
        List<DataPoint> dataPoints = createTenDataPoints();
        dataPoints.forEach(dp -> siDiffTimestampModelType.append(dp));
        siDiffTimestampModelType.reduceToSizeN(5);
        int actualAmountOfTimestamps = siDiffTimestampModelType.getLength();

        // Assert list has 5 data points
        assertEquals(5, actualAmountOfTimestamps);
    }

    @Test
    public void testReduceSizeWithNumberOfTimestamps(){
        List<DataPoint> dataPoints = createTenDataPoints();
        dataPoints.forEach(dp -> siDiffTimestampModelType.append(dp));
        int expectedAmountTimestamps = dataPoints.size();
        siDiffTimestampModelType.reduceToSizeN(expectedAmountTimestamps);

        assertEquals(expectedAmountTimestamps, siDiffTimestampModelType.getLength());
    }

    @Test
    public void testReduceSizeWithZeroThrowsException(){
        List<DataPoint> dataPoints = createTenDataPoints();
        dataPoints.forEach(dp -> siDiffTimestampModelType.append(dp));
        assertThrows(IllegalArgumentException.class, () -> siDiffTimestampModelType.reduceToSizeN(0));
    }

    @Test
    public void testReduceSizeWithMoreThanListSizeThrowsException(){
        List<DataPoint> dataPoints = createTenDataPoints();
        dataPoints.forEach(dp -> siDiffTimestampModelType.append(dp));
        assertThrows(IllegalArgumentException.class, () -> siDiffTimestampModelType.reduceToSizeN(20));
    }

    @Test
    public void testBlobRepresentation1(){
        List<DataPoint> dataPoints = createTenDataPoints();

        dataPoints.forEach(dp -> siDiffTimestampModelType.append(dp));

        var blobRepresentation = siDiffTimestampModelType.getBlobRepresentation();

        var decodedValues = BlobDecompressor.decompressTimestamps(TimestampCompressionModelType.SIDIFF,
                blobRepresentation, dataPoints.get(0).timestamp(), dataPoints.get(dataPoints.size() - 1).timestamp());

        for (int i = 0; i < decodedValues.size(); i++){
            Assertions.assertEquals(dataPoints.get(i).timestamp(), decodedValues.get(i));
        }
    }
}