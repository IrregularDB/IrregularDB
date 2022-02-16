package compression.value;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PMCMeanCompressionModelTest {
    PMCMeanCompressionModel pmcMeanModel;

    @BeforeEach
    void init() {
        double errorBound = 10;
        pmcMeanModel = new PMCMeanCompressionModel(errorBound);
    }

    @Test
    void getLength() {
        List<Double> values = Arrays.asList(1.0, 1.0, 1.0);

        Assertions.assertEquals(0, pmcMeanModel.getLength());
        pmcMeanModel.appendValue(values.get(0));
        Assertions.assertEquals(1, pmcMeanModel.getLength());
        pmcMeanModel.appendValue(values.get(1));
        pmcMeanModel.appendValue(values.get(2));
        Assertions.assertEquals(3, pmcMeanModel.getLength());
    }

    @Test
    void appendOneValue() {
        Assertions.assertTrue(pmcMeanModel.appendValue(1.0));
    }

    @Test
    void appendTwoValues() {
        Assertions.assertTrue(pmcMeanModel.appendValue(1.00));
        Assertions.assertTrue(pmcMeanModel.appendValue(1.05));
    }

    @Test
    void appendVeryDifferentValue() {
        Assertions.assertTrue(pmcMeanModel.appendValue(1.00));
        Assertions.assertFalse(pmcMeanModel.appendValue(9.00));
    }

    @Test
    void appendAfterFailedAppendNotAllowed() {
        pmcMeanModel.appendValue(1.00);
        pmcMeanModel.appendValue(9.00);

        Assertions.assertThrows(IllegalArgumentException.class, () -> pmcMeanModel.appendValue(1.00));
    }

    @Test
    void resetAndAppendAllEmptyModel() {
        // We test what happens when we append to an empty model
        List<Double> values = Arrays.asList(1.0, 1.0, 1.0);
        Assertions.assertTrue(pmcMeanModel.resetAndAppendAll(values));
    }


    @Test
    void resetAndAppendAllModelWithData() {
        // Here we expect it to be able to append 3 data points even though they are very
        // different compared to the old ones as it should be reset

        List<Double> values = Arrays.asList(1.0, 1.0, 1.0);
        pmcMeanModel.resetAndAppendAll(values);
        values = Arrays.asList(99.0, 99.0, 99.9);
        Assertions.assertTrue(pmcMeanModel.resetAndAppendAll(values));
    }

    @Test
    void getValueBlobEmptyModel() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> pmcMeanModel.getValueBlob());
    }

    @Test
    void getValueBlobNonEmptyModel() {
        // We expect a model with 1.05 as mean value
        pmcMeanModel.appendValue(1.00);
        pmcMeanModel.appendValue(1.10);

        ByteBuffer valueBlob = pmcMeanModel.getValueBlob();
        float meanValue = valueBlob.getFloat(0);
        assertEquals(1.05F, meanValue);
    }


    @Test
    void getCompressionRatio() {
        // TODO: implement this
    }

    @Test
    void reduceToSizeN() {
        // TODO: implement this
    }
}