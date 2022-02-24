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
    private List<DataPoint> createDataPointsFromValues(List<Float> values) {
        List<DataPoint> dataPoints = new ArrayList<>();
        int i = 0;
        for (var value : values) {
            dataPoints.add(new DataPoint(i, value));
            i++;
        }
        return dataPoints;
    }

    @BeforeEach
    void init() {
        float errorBound = 10;
        swingModel = new SwingValueCompressionModel(errorBound);
    }

    @Test
    void appendTwoValues() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F));

        Assertions.assertTrue(swingModel.append(dataPoints.get(0)));
        Assertions.assertTrue(swingModel.append(dataPoints.get(1)));
    }

    @Test
    void appendThreeValues() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F));

        Assertions.assertTrue(swingModel.append(dataPoints.get(0)));
        Assertions.assertTrue(swingModel.append(dataPoints.get(1)));
        Assertions.assertTrue(swingModel.append(dataPoints.get(2)));
    }

    @Test
    void appendVeryDifferentValue() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 9.99F));

        Assertions.assertTrue(swingModel.append(dataPoints.get(0)));
        Assertions.assertTrue(swingModel.append(dataPoints.get(1)));
        Assertions.assertFalse(swingModel.append(dataPoints.get(2)));
        Assertions.assertEquals(2, swingModel.getLength());
    }

    @Test
    void appendAfterFailedAppendNotAllowed() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 9.99F, 1.10F));

        swingModel.append(dataPoints.get(0));
        swingModel.append(dataPoints.get(1));
        swingModel.append(dataPoints.get(2));
        Assertions.assertThrows(IllegalArgumentException.class, () -> swingModel.append(dataPoints.get(3)));
    }

    @Test
    void getLength() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F));

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
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F, 1.14F, 1.21F, 1.28F, 1.32F));

        Assertions.assertTrue(swingModel.resetAndAppendAll(dataPoints));
    }

    @Test
    void resetAndAppendAllNonRegularTimeStamps() {
        List<DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(new DataPoint(0, 1.00F));
        dataPoints.add(new DataPoint(3, 1.03F));
        dataPoints.add(new DataPoint(7, 1.07F));
        dataPoints.add(new DataPoint(17, 1.20F));
        dataPoints.add(new DataPoint(35, 1.40F));

        Assertions.assertTrue(swingModel.resetAndAppendAll(dataPoints));
    }

    @Test
    void resetAndAppendNegativeSlope() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(10.00F, 9.00F, 8.00F, 7.00F, 6.00F, 5.00F));
        Assertions.assertTrue(swingModel.resetAndAppendAll(dataPoints));
    }

    @Test
    void resetAndAppendAllNonEmptyModel() {
        // Here we expect it to be able to append 3 data points even though they are very
        // different compared to the old ones as it should be reset
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F, 1.20F));

        swingModel.resetAndAppendAll(dataPoints);
        dataPoints = createDataPointsFromValues(Arrays.asList(99.9F, 99.9F, 99.9F));
        Assertions.assertTrue(swingModel.resetAndAppendAll(dataPoints));
    }

    @Test
    void resetAndAppendAllWhereSomePointCannotBeRepresented() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F, 1.20F, 99.9F, 99.9F));
        Assertions.assertFalse(swingModel.resetAndAppendAll(dataPoints));
        Assertions.assertEquals(5, swingModel.getLength());
    }

    @Test
    void noErrorBoundAppendAllTest() {
        float errorBound = 0;
        swingModel = new SwingValueCompressionModel(errorBound);
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F, 1.20F));
        Assertions.assertTrue(swingModel.resetAndAppendAll(dataPoints));
    }

    @Test
    void noErrorBoundAppendAllSmallErrorNotAllowedTest() {
        float errorBound = 0;
        swingModel = new SwingValueCompressionModel(errorBound);
        // 1.26 is slightly off from 1.25 thereby not allowed.
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F, 1.20F, 1.26F));
        Assertions.assertFalse(swingModel.resetAndAppendAll(dataPoints));
        Assertions.assertEquals(5, swingModel.getLength());
    }


    @Test
    void effectOfSwingUp() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F));

        swingModel.resetAndAppendAll(dataPoints);
        // The initial lower bound is: -0.05x + 1.
        // So if we did not swing the lower bound up we would be allowed to do the following
        assertFalse(swingModel.append(new DataPoint(20, 0)));
    }

    @Test
    void effectOfSwingDown() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F));
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
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F, 1.20F));
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
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F));
        swingModel.resetAndAppendAll(dataPoints);

        assertEquals(0.25, swingModel.getCompressionRatio());
    }

    @Test
    void getCompressionRatio4DataPoints() {
        // We use 8 bytes to represent 4 data points so 4/8 = 0.5
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F));
        swingModel.resetAndAppendAll(dataPoints);

        assertEquals(0.5, swingModel.getCompressionRatio());
    }

    @Test
    void getCompressionRatio8DataPoints() {
        // We use 8 bytes to represent 8 data points so 8/8 = 0.5
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F, 1.14F, 1.21F, 1.28F, 1.32F));
        swingModel.resetAndAppendAll(dataPoints);

        assertEquals(1, swingModel.getCompressionRatio());
    }
}