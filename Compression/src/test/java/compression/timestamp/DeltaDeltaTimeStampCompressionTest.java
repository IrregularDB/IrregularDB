package compression.timestamp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import records.DataPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

class DeltaDeltaTimeStampCompressionTest {

    Random random = new Random();
    long previousLong;
    float previousFloat;
    List<DataPoint> dataPoints = createTenDataPoints();
    private DeltaDeltaTimestampCompression deltaDeltaTimeStampCompression;

    @BeforeEach
    void beforeEach(){
        deltaDeltaTimeStampCompression = new DeltaDeltaTimestampCompression(0);
    }

    @Test
    public void testModelAppendsTwice(){
        boolean append1 = deltaDeltaTimeStampCompression.append(createDataPoint());
        boolean append2 = deltaDeltaTimeStampCompression.append(createDataPoint());

        Assertions.assertTrue(append1);
        Assertions.assertTrue(append2);
    }

    @Test
    public void testModelResets(){
        deltaDeltaTimeStampCompression.append(createDataPoint());
        deltaDeltaTimeStampCompression.append(createDataPoint());
        deltaDeltaTimeStampCompression.append(createDataPoint());

        List<DataPoint> dataPointList = createTenDataPoints();
        boolean success = deltaDeltaTimeStampCompression.resetAndAppendAll(dataPointList);
        int actualLength = deltaDeltaTimeStampCompression.getLength();

        Assertions.assertTrue(success);
        Assertions.assertEquals(10, actualLength);
    }

    @Test
    public void testReduceSizeToN(){
        dataPoints.forEach(dp -> deltaDeltaTimeStampCompression.append(dp));
        deltaDeltaTimeStampCompression.reduceToSizeN(5);
        int actualAmountOfTimeStamps = deltaDeltaTimeStampCompression.getLength();

        // Assert list has 5 data points
        Assertions.assertEquals(5, actualAmountOfTimeStamps);
    }

    @Test
    public void testReduceSizeWithNumberOfTimeStamps(){
        dataPoints.forEach(dp -> deltaDeltaTimeStampCompression.append(dp));
        int expectedAmountTimestamps = dataPoints.size();
        deltaDeltaTimeStampCompression.reduceToSizeN(expectedAmountTimestamps);

        Assertions.assertEquals(expectedAmountTimestamps, deltaDeltaTimeStampCompression.getLength());
    }

    @Test
    public void testReduceSizeWithZeroThrowsException(){
        dataPoints.forEach(dp -> deltaDeltaTimeStampCompression.append(dp));
        Assertions.assertThrows(IllegalArgumentException.class, () -> deltaDeltaTimeStampCompression.reduceToSizeN(0));
    }

    @Test
    public void testReduceSizeWithMoreThanListSizeThrowsException(){
        dataPoints.forEach(dp -> deltaDeltaTimeStampCompression.append(dp));
        Assertions.assertThrows(IllegalArgumentException.class, () -> deltaDeltaTimeStampCompression.reduceToSizeN(20));
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