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

class PMCMeanValueCompressionModelTest {
    PMCMeanValueCompressionModel pmcMeanModel;

    // Helper method needed to be able to use reset and append all as it now takes data points
    private List<DataPoint> createDataPointsFromValues(List<Double> values) {
        List<DataPoint> dataPoints = new ArrayList<>();
        for (Double value : values) {
            dataPoints.add(createDataPointForValue(value));
        }
        return dataPoints;
    }

    private DataPoint createDataPointForValue(double value) {
        // We use -1 for our data points as timestamp because this model does not care about the values of the data points
        return new DataPoint(-1, value);
    }

    @BeforeEach
    void init() {
        double errorBound = 10;
        pmcMeanModel = new PMCMeanValueCompressionModel(errorBound);
    }

    @Test
    void appendOneValue() {
        Assertions.assertTrue(pmcMeanModel.append(createDataPointForValue(1.0)));
    }

    @Test
    void appendTwoValues() {
        Assertions.assertTrue(pmcMeanModel.append(createDataPointForValue(1.00)));
        Assertions.assertTrue(pmcMeanModel.append(createDataPointForValue(1.05)));
    }

    @Test
    void appendVeryDifferentValue() {
        Assertions.assertTrue(pmcMeanModel.append(createDataPointForValue(1.00)));
        Assertions.assertFalse(pmcMeanModel.append(createDataPointForValue(9.00)));
    }

    @Test
    void appendAfterFailedAppendNotAllowed() {
        pmcMeanModel.append(createDataPointForValue(1.00));
        pmcMeanModel.append(createDataPointForValue(9.00));

        Assertions.assertThrows(IllegalArgumentException.class, () -> pmcMeanModel.append(createDataPointForValue(1.00)));
    }

    @Test
    void getLength() {
        Assertions.assertEquals(0, pmcMeanModel.getLength());
        pmcMeanModel.append(createDataPointForValue(1.00));
        Assertions.assertEquals(1, pmcMeanModel.getLength());
        pmcMeanModel.append(createDataPointForValue(1.00));
        pmcMeanModel.append(createDataPointForValue(1.00));
        Assertions.assertEquals(3, pmcMeanModel.getLength());
    }

    @Test
    void resetAndAppendAllEmptyModel() {
        // We test what happens when we append to an table.sql model
        List<Double> values = Arrays.asList(1.0, 1.0, 1.0);
        Assertions.assertTrue(pmcMeanModel.resetAndAppendAll(createDataPointsFromValues(values)));
    }


    @Test
    void resetAndAppendAllNonEmptyModel() {
        // Here we expect it to be able to append 3 data points even though they are very
        // different compared to the old ones as it should be reset
        List<Double> values = Arrays.asList(1.0, 1.0, 1.0);
        pmcMeanModel.resetAndAppendAll(createDataPointsFromValues(values));
        values = Arrays.asList(99.0, 99.0, 99.9);
        Assertions.assertTrue(pmcMeanModel.resetAndAppendAll(createDataPointsFromValues(values)));
    }

    @Test
    void resetAndAppendAllWhereSomePointCannotBeRepresented() {
        List<Double> values = Arrays.asList(1.0, 1.0, 1.0, 99.0, 99.0);
        Assertions.assertFalse(pmcMeanModel.resetAndAppendAll(createDataPointsFromValues(values)));
        Assertions.assertEquals(3, pmcMeanModel.getLength());
    }


    @Test
    void getValueBlobEmptyModel() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> pmcMeanModel.getBlobRepresentation());
    }

    @Test
    void getValueBlobNonEmptyModel() {
        // We expect a model with 1.05 as mean value
        pmcMeanModel.append(createDataPointForValue(1.00));
        pmcMeanModel.append(createDataPointForValue(1.10));

        ByteBuffer valueBlob = pmcMeanModel.getBlobRepresentation();
        float meanValue = valueBlob.getFloat(0);
        assertEquals(1.05F, meanValue);
    }

    @Test
    void getCompressionRatio2DataPoints() {
        // We expect that we have used 4 bytes to represent 2 data points so we get 2/4 = 0.5
        List<Double> values = Arrays.asList(1.00, 1.00);
        pmcMeanModel.resetAndAppendAll(createDataPointsFromValues(values));

        assertEquals(0.5, pmcMeanModel.getCompressionRatio());
    }

    @Test
    void getCompressionRatio4DataPoints() {
        // We expect that we have used 4 bytes to represent 4 data points so we get 4/4 = 1
        List<Double> values = Arrays.asList(1.00, 1.00, 1.00, 1.00);
        pmcMeanModel.resetAndAppendAll(createDataPointsFromValues(values));

        assertEquals(1.0, pmcMeanModel.getCompressionRatio());
    }

    @Test
    void getCompressionRatio8DataPoints() {
        // We expect that we have used 4 bytes to represent 8 data points so we get 8/4 = 2.0
        List<Double> values = Arrays.asList(1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00);
        pmcMeanModel.resetAndAppendAll(createDataPointsFromValues(values));

        assertEquals(2, pmcMeanModel.getCompressionRatio());
    }

    @Test
    void reduceToSizeN() {
        // TODO: implement reduce to N test
    }
}