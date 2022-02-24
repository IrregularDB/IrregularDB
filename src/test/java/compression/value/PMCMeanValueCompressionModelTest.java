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
    private List<DataPoint> createDataPointsFromValues(List<Float> values) {
        List<DataPoint> dataPoints = new ArrayList<>();
        for (var value : values) {
            dataPoints.add(createDataPointForValue(value));
        }
        return dataPoints;
    }

    private DataPoint createDataPointForValue(float value) {
        // We use -1 for our data points as timestamp because this model does not care about the values of the data points
        return new DataPoint(-1, value);
    }

    @BeforeEach
    void init() {
        float errorBound = 10;
        pmcMeanModel = new PMCMeanValueCompressionModel(errorBound);
    }

    @Test
    void appendOneValue() {
        Assertions.assertTrue(pmcMeanModel.append(createDataPointForValue(1.0F)));
    }

    @Test
    void appendTwoValues() {
        Assertions.assertTrue(pmcMeanModel.append(createDataPointForValue(1.00F)));
        Assertions.assertTrue(pmcMeanModel.append(createDataPointForValue(1.05F)));
    }

    @Test
    void appendVeryDifferentValue() {
        Assertions.assertTrue(pmcMeanModel.append(createDataPointForValue(1.00F)));
        Assertions.assertFalse(pmcMeanModel.append(createDataPointForValue(9.00F)));
    }

    @Test
    void appendAfterFailedAppendNotAllowed() {
        pmcMeanModel.append(createDataPointForValue(1.00F));
        pmcMeanModel.append(createDataPointForValue(9.00F));

        Assertions.assertThrows(IllegalArgumentException.class, () -> pmcMeanModel.append(createDataPointForValue(1.00F)));
    }

    @Test
    void getLength() {
        Assertions.assertEquals(0, pmcMeanModel.getLength());
        pmcMeanModel.append(createDataPointForValue(1.00F));
        Assertions.assertEquals(1, pmcMeanModel.getLength());
        pmcMeanModel.append(createDataPointForValue(1.00F));
        pmcMeanModel.append(createDataPointForValue(1.00F));
        Assertions.assertEquals(3, pmcMeanModel.getLength());
    }

    @Test
    void resetAndAppendAllEmptyModel() {
        // We test what happens when we append to an empty model
        var values = Arrays.asList(1.0F, 1.0F, 1.0F);
        Assertions.assertTrue(pmcMeanModel.resetAndAppendAll(createDataPointsFromValues(values)));
    }


    @Test
    void resetAndAppendAllNonEmptyModel() {
        // Here we expect it to be able to append 3 data points even though they are very
        // different compared to the old ones as it should be reset
        var values = Arrays.asList(1.0F, 1.0F, 1.0F);
        pmcMeanModel.resetAndAppendAll(createDataPointsFromValues(values));
        values = Arrays.asList(99.0F, 99.0F, 99.9F);
        Assertions.assertTrue(pmcMeanModel.resetAndAppendAll(createDataPointsFromValues(values)));
    }

    @Test
    void resetAndAppendAllWhereSomePointCannotBeRepresented() {
        var values = Arrays.asList(1.0F, 1.0F, 1.0F, 99.0F, 99.0F);
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
        pmcMeanModel.append(createDataPointForValue(1.00F));
        pmcMeanModel.append(createDataPointForValue(1.10F));

        ByteBuffer valueBlob = pmcMeanModel.getBlobRepresentation();
        float meanValue = valueBlob.getFloat(0);
        assertEquals(1.05F, meanValue);
    }

    @Test
    void getAmountOfBytesUsed0DataPoints() {
        // We expect this to throw and exception as no model has been made yet.
        Assertions.assertThrows(UnsupportedOperationException.class, () -> pmcMeanModel.getAmountBytesUsed());
    }

    @Test
    void getAmountOfBytesUsed() {
        var values = Arrays.asList(1.00F, 1.00F);
        pmcMeanModel.resetAndAppendAll(createDataPointsFromValues(values));
        // We expect that we use 4 bytes
        Assertions.assertEquals(4, pmcMeanModel.getAmountBytesUsed());
    }

    @Test
    void reduceToSizeN() {
        var values = Arrays.asList(1.0F, 1.0F, 1.0F, 1.0F);
        pmcMeanModel.resetAndAppendAll(createDataPointsFromValues(values));
        pmcMeanModel.reduceToSizeN(2);
        Assertions.assertEquals(2, pmcMeanModel.getLength());
    }

    @Test
    void reduceToSizeNIllegalArgument() {
        var values = Arrays.asList(1.0F, 1.0F, 1.0F, 1.0F);
        pmcMeanModel.resetAndAppendAll(createDataPointsFromValues(values));
        Assertions.assertThrows(IllegalArgumentException.class, () ->  pmcMeanModel.reduceToSizeN(5));
    }
}