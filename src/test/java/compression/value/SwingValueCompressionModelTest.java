package compression.value;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SwingValueCompressionModelTest {
    SwingValueCompressionModel swingModel;

    // Helper method needed to be able to use reset and append all as it now takes data points
    private List<DataPoint> createDataPointsFromValues(List<Double> values) {
        List<DataPoint> dataPoints = new ArrayList<>();
        int i = 0;
        for (Double value : values) {
            dataPoints.add(new DataPoint(i, value));
            i++;
        }
        return dataPoints;
    }

    @BeforeEach
    void init() {
        double errorBound = 10;
        swingModel = new SwingValueCompressionModel(errorBound);
    }

    @Test
    void appendTwoValues() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00, 1.05));

        Assertions.assertTrue(swingModel.append(dataPoints.get(0)));
        Assertions.assertTrue(swingModel.append(dataPoints.get(1)));
    }

    @Test
    void appendThreeValues() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00, 1.05, 1.10));

        Assertions.assertTrue(swingModel.append(dataPoints.get(0)));
        Assertions.assertTrue(swingModel.append(dataPoints.get(1)));
        Assertions.assertTrue(swingModel.append(dataPoints.get(2)));
    }

    @Test
    void appendVeryDifferentValue() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00, 1.05, 9.99));

        Assertions.assertTrue(swingModel.append(dataPoints.get(0)));
        Assertions.assertTrue(swingModel.append(dataPoints.get(1)));
        Assertions.assertFalse(swingModel.append(dataPoints.get(2)));
    }

    @Test
    void appendAfterFailedAppendNotAllowed() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00, 1.05, 9.99, 1.10));

        swingModel.append(dataPoints.get(0));
        swingModel.append(dataPoints.get(1));
        swingModel.append(dataPoints.get(2));
        Assertions.assertThrows(IllegalArgumentException.class, () -> swingModel.append(dataPoints.get(3)));
    }

    @Test
    void getLength() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00, 1.05, 1.10));

        Assertions.assertEquals(0, swingModel.getLength());
        swingModel.append(dataPoints.get(0));
        Assertions.assertEquals(1, swingModel.getLength());
        swingModel.append(dataPoints.get(1));
        swingModel.append(dataPoints.get(2));
        Assertions.assertEquals(3, swingModel.getLength());
    }

    @Test
    void resetAndAppendAllEmptyModel() {
        // We test what happens when we append to an empty model
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00, 1.05, 1.10, 1.15, 1.14, 1.21, 1.28, 1.32));

        Assertions.assertTrue(swingModel.resetAndAppendAll(dataPoints));
    }

    @Test
    void resetAndAppendAllNonRegularTimeStamps() {
        List<DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(new DataPoint(0, 1.00));
        dataPoints.add(new DataPoint(3, 1.03));
        dataPoints.add(new DataPoint(7, 1.07));
        dataPoints.add(new DataPoint(17, 1.20));
        dataPoints.add(new DataPoint(35, 1.40));

        Assertions.assertTrue(swingModel.resetAndAppendAll(dataPoints));
    }

    @Test
    void resetAndAppendAllNonEmptyModel() {
        // Here we expect it to be able to append 3 data points even though they are very
        // different compared to the old ones as it should be reset
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00, 1.05, 1.10, 1.15, 1.20));

        swingModel.resetAndAppendAll(dataPoints);
        dataPoints = createDataPointsFromValues(Arrays.asList(99.9, 99.9, 99.9));
        Assertions.assertTrue(swingModel.resetAndAppendAll(dataPoints));
    }



    @Test
    void resetAndAppendAllWhereSomePointCannotBeRepresented() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00, 1.05, 1.10, 1.15, 1.20, 99.9, 99.9));
        Assertions.assertFalse(swingModel.resetAndAppendAll(dataPoints));
        Assertions.assertEquals(5, swingModel.getLength());
    }


    @Test
    void effectOfSwingUp() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00, 1.05, 1.10, 1.15));

        swingModel.resetAndAppendAll(dataPoints);
        // The initial lower bound is: -0.05x + 1.
        // So if we did not swing the lower bound up we would be allowed to do the following
        assertFalse(swingModel.append(new DataPoint(20, 0)));
    }

    @Test
    void effectOfSwingDown() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00, 1.05, 1.10, 1.15));
        swingModel.resetAndAppendAll(dataPoints);
        // The initial upper bound is: 0.15x + 1.
        // So if we did not swing the lower bound up we would be allowed to do the following
        assertFalse(swingModel.append(new DataPoint(20, 4)));

    }



    @Test
    void getValueBlobEmptyModel() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> swingModel.getBlobRepresentation());
    }

    @Test
    void getValueBlobNonEmptyModel() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00, 1.05, 1.10, 1.15, 1.20));
        swingModel.resetAndAppendAll(dataPoints);
        ByteBuffer valueBlob = swingModel.getBlobRepresentation();
        var slope = valueBlob.getFloat(0);
        var intercept = valueBlob.getFloat(4);

        var allowedDifference = 0.000000000001;
        assertTrue(0.05F - slope < allowedDifference);
        assertTrue(1.00F - intercept < allowedDifference);
    }

    @Test
    void getCompressionRatio2DataPoints() {
        // We use 8 bytes to represent 2 data points so 2/8 = 0.25
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00, 1.05));
        swingModel.resetAndAppendAll(dataPoints);

        assertEquals(0.25, swingModel.getCompressionRatio());
    }

    @Test
    void getCompressionRatio4DataPoints() {
        // We use 8 bytes to represent 4 data points so 4/8 = 0.5
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00, 1.05, 1.10, 1.15));
        swingModel.resetAndAppendAll(dataPoints);

        assertEquals(0.5, swingModel.getCompressionRatio());
    }

    @Test
    void getCompressionRatio8DataPoints() {
        // We use 8 bytes to represent 8 data points so 8/8 = 0.5
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00, 1.05, 1.10, 1.15, 1.14, 1.21, 1.28, 1.32));
        swingModel.resetAndAppendAll(dataPoints);

        assertEquals(1, swingModel.getCompressionRatio());
    }

    @Test
    void reduceToSizeN() {
        // TODO: implement reduce to N test
    }
}