package compression.timestamp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class BaseDeltaPairsTimeStampCompressionModelTest {

    BaseDeltaTimeStampCompressionModel baseDeltaTimeStampCompressionModel;
    Random random = new Random();
    long previousLong;
    float previousFloat;
    List<DataPoint> dataPoints = createTenDataPoints();

    @BeforeEach
    void init() {
        baseDeltaTimeStampCompressionModel = new BaseDeltaTimeStampCompressionModel();
    }

    @Test
    public void testModelAppendsTwice(){
        boolean append1 = baseDeltaTimeStampCompressionModel.append(createDataPoint());
        boolean append2 = baseDeltaTimeStampCompressionModel.append(createDataPoint());

        Assertions.assertTrue(append1);
        Assertions.assertTrue(append2);
    }

    @Test
    public void testModelResets(){
        baseDeltaTimeStampCompressionModel.append(createDataPoint());
        baseDeltaTimeStampCompressionModel.append(createDataPoint());
        baseDeltaTimeStampCompressionModel.append(createDataPoint());

        List<DataPoint> dataPointList = createTenDataPoints();
        boolean success = baseDeltaTimeStampCompressionModel.resetAndAppendAll(dataPointList);
        int actualLength = baseDeltaTimeStampCompressionModel.getLength();

        Assertions.assertTrue(success);
        Assertions.assertEquals(9, actualLength);
    }

    @Test
    public void testBlobRepresentation(){
        baseDeltaTimeStampCompressionModel.append(new DataPoint(0, 1.1f));
        baseDeltaTimeStampCompressionModel.append(new DataPoint(1, 2.2f));
        baseDeltaTimeStampCompressionModel.append(new DataPoint(2, 3.3f));

        ByteBuffer blobRepresentation = baseDeltaTimeStampCompressionModel.getBlobRepresentation();

        // base value not stored in byte buffer
        int actualFirstTimeStamp = blobRepresentation.getInt(0);
        int actualSecondTimeStamp = blobRepresentation.getInt(4);

        Assertions.assertEquals(1, actualFirstTimeStamp);
        Assertions.assertEquals(2, actualSecondTimeStamp);
    }

    @Test
    public void testReduceSizeToN(){
        dataPoints.forEach(dp -> baseDeltaTimeStampCompressionModel.append(dp));
        baseDeltaTimeStampCompressionModel.reduceToSizeN(5);
        int actualAmountOfTimeStamps = baseDeltaTimeStampCompressionModel.getLength();

        // Assert list has 5 data points
        Assertions.assertEquals(5, actualAmountOfTimeStamps);
    }

    @Test
    public void testReduceSizeWithNumberOfTimeStamps(){
        dataPoints.forEach(dp -> baseDeltaTimeStampCompressionModel.append(dp));
        baseDeltaTimeStampCompressionModel.reduceToSizeN(9);
        int actualAmountOfTimeStamps = baseDeltaTimeStampCompressionModel.getLength();

        Assertions.assertEquals(9, actualAmountOfTimeStamps);
    }

    @Test
    public void testReduceSizeWithZeroThrowsException(){
        dataPoints.forEach(dp -> baseDeltaTimeStampCompressionModel.append(dp));
        Assertions.assertThrows(IllegalArgumentException.class, () -> baseDeltaTimeStampCompressionModel.reduceToSizeN(0));
    }

    @Test
    public void testReduceSizeWithMoreThanListSizeThrowsException(){
        dataPoints.forEach(dp -> baseDeltaTimeStampCompressionModel.append(dp));
        Assertions.assertThrows(IllegalArgumentException.class, () -> baseDeltaTimeStampCompressionModel.reduceToSizeN(20));
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