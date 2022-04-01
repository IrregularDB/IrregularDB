package compression.timestamp;

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
        deltaDeltaTimestampCompressionModel = new DeltaDeltaTimestampCompressionModel(0);
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