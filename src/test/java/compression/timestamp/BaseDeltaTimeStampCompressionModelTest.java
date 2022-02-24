package compression.timestamp;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class BaseDeltaTimeStampCompressionModelTest {

    BaseDeltaTimeStampCompressionModel baseDeltaTimeStampCompressionModel;
    Random random = new Random();
    long previousLong;
    double previousDouble;
    List<DataPoint> dataPoints = createTenDataPoints();

    @BeforeEach
    void init() {
        baseDeltaTimeStampCompressionModel = new BaseDeltaTimeStampCompressionModel(0.0);
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

        Assertions.assertTrue(success);
    }

    @Test
    public void testBlobRepresentation(){
        baseDeltaTimeStampCompressionModel.append(new DataPoint(0, 1.1));
        baseDeltaTimeStampCompressionModel.append(new DataPoint(1, 2.2));
        baseDeltaTimeStampCompressionModel.append(new DataPoint(2, 3.3));

        ByteBuffer blobRepresentation = baseDeltaTimeStampCompressionModel.getBlobRepresentation();

        // First value is not stored in the byte buffer
        int actualFirstTimeStamp = blobRepresentation.getInt(0);
        int actualSecondTimeStamp = blobRepresentation.getInt(4);

        assertEquals(0, baseDeltaTimeStampCompressionModel.getStartTime());
        assertEquals(1, actualFirstTimeStamp);
        assertEquals(2, actualSecondTimeStamp);
    }

    @Test
    public void testReduceSizeToN(){
        dataPoints.forEach(dp -> baseDeltaTimeStampCompressionModel.append(dp));

        baseDeltaTimeStampCompressionModel.reduceToSizeN(5);

        int actualAmountOfTimeStamps = baseDeltaTimeStampCompressionModel.getTimeStampsAmount();

        // Assert list has 5 data points
        assertEquals(5, actualAmountOfTimeStamps);
    }

    @Test
    public void testReduceSizeWithNumberOfTimeStamps(){
        dataPoints.forEach(dp -> baseDeltaTimeStampCompressionModel.append(dp));

        baseDeltaTimeStampCompressionModel.reduceToSizeN(9);

        int actualAmountOfTimeStamps = baseDeltaTimeStampCompressionModel.getTimeStampsAmount();

        assertEquals(9, actualAmountOfTimeStamps);
    }

    @Test
    public void testReduceSizeWithZeroThrowsException(){
        dataPoints.forEach(dp -> baseDeltaTimeStampCompressionModel.append(dp));

        assertThrows(IllegalArgumentException.class, () -> baseDeltaTimeStampCompressionModel.reduceToSizeN(0));
    }

    @Test
    public void testReduceSizeWithMoreThanListSizeThrowsException(){
        dataPoints.forEach(dp -> baseDeltaTimeStampCompressionModel.append(dp));

        assertThrows(IllegalArgumentException.class, () -> baseDeltaTimeStampCompressionModel.reduceToSizeN(20));
    }


    // Helper that creates random data points in increasing order
    private DataPoint createDataPoint(){
        previousLong += random.nextLong(100L);
        previousDouble += random.nextDouble(100.00);
        return new DataPoint(previousLong, previousDouble);
    }

    private List<DataPoint> createTenDataPoints(){
        List<DataPoint> dataPointList = new ArrayList<>();
        for(int i = 0; i < 10; i++){
            dataPointList.add(createDataPoint());
        }
        return dataPointList;
    }

}