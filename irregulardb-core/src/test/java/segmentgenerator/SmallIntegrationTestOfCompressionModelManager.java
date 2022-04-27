package segmentgenerator;

import compression.timestamp.*;
import compression.value.*;
import config.ConfigProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import records.CompressionModel;
import records.DataPoint;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SmallIntegrationTestOfCompressionModelManager {
    CompressionModelManager compressionModelManager;
    private static final int LENGTH_BOUND = 3;

    @BeforeAll
    static void beforeAll(){
        ConfigProperties.isTest = true;
        ConfigProperties.getInstance().setProperty("model.length_bound", String.valueOf(LENGTH_BOUND));
    }

    @BeforeEach
    void beforeEach() {
        float errorBound = 0;
        List<ValueCompressionModel> valueModels = new ArrayList<>();
        valueModels.add(new PMCMeanValueCompressionModel(errorBound));
        valueModels.add(new SwingValueCompressionModel(errorBound));
        valueModels.add(new GorillaValueCompressionModel(LENGTH_BOUND));

        int threshold = 0;
        List<TimestampCompressionModel> timestampModels = new ArrayList<>();
        timestampModels.add(new RegularTimestampCompressionModel(threshold));
        timestampModels.add(new SIDiffTimestampCompressionModel(threshold, LENGTH_BOUND));
        timestampModels.add(new DeltaDeltaTimestampCompressionModel(threshold, LENGTH_BOUND));

        ModelPicker modelPicker = ModelPickerFactory.createModelPicker(ModelPickerFactory.ModelPickerType.BRUTE_FORCE);
        compressionModelManager = new CompressionModelManager(valueModels, timestampModels, modelPicker);
    }


    // In this case we expect the model to fit all the first 3 data points and then fail at the sixth one
    // as our length bound is 3 and neither PMC-mean or SWING can fit the 6th point
    @Test
    void tryAppendDataPointToAllModels() {
        List<DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(new DataPoint(0, 1));
        dataPoints.add(new DataPoint(100, 1));
        dataPoints.add(new DataPoint(200, 1));
        dataPoints.add(new DataPoint(300, 1));
        dataPoints.add(new DataPoint(400, 1));
        dataPoints.add(new DataPoint(500, 1));
        dataPoints.add(new DataPoint(600, 999));

        for (int i = 0; i < dataPoints.size() - 1; i++) {
            Assertions.assertTrue(compressionModelManager.tryAppendDataPointToAllModels(dataPoints.get(i)));
        }
        DataPoint lastDataPoint = dataPoints.get(dataPoints.size() - 1);
        assertFalse(compressionModelManager.tryAppendDataPointToAllModels(lastDataPoint));
    }


    @Test
    void resetAndTryAppendBuffer() {
        List<DataPoint> initialDataPoints = new ArrayList<>();
        initialDataPoints.add(new DataPoint(0, 1));
        initialDataPoints.add(new DataPoint(100, 1));
        initialDataPoints.add(new DataPoint(200, 1));
        for (DataPoint dataPoint : initialDataPoints) {
            Assertions.assertTrue(compressionModelManager.tryAppendDataPointToAllModels(dataPoint));
        }

        // Even though these new data points are very different from the old one we expect the
        // the method to return true as the models should have been reseted.

        List<DataPoint> buffer = new ArrayList<>();
        buffer.add(new DataPoint(1000, 999));
        buffer.add(new DataPoint(1100, 999));
        buffer.add(new DataPoint(1200, 999));
        buffer.add(new DataPoint(1300, 999));
        Assertions.assertTrue(compressionModelManager.resetAndTryAppendBuffer(buffer));
    }


    // TODO: consider adding more tests using other models
    // We here expect a regular model and a pmc mean model with length 10
    @Test
    void getBestCompressionModelPmcMeanAndRegular() {
        List<DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(new DataPoint(0, 1));
        dataPoints.add(new DataPoint(100, 1));
        dataPoints.add(new DataPoint(200, 1));
        dataPoints.add(new DataPoint(300, 1));
        dataPoints.add(new DataPoint(400, 1));
        dataPoints.add(new DataPoint(500, 1));
        dataPoints.add(new DataPoint(600, 1));
        dataPoints.add(new DataPoint(700, 1));
        dataPoints.add(new DataPoint(800, 1));
        dataPoints.add(new DataPoint(900, 1));

        for (DataPoint dataPoint : dataPoints) {
            Assertions.assertTrue(compressionModelManager.tryAppendDataPointToAllModels(dataPoint));
        }

        CompressionModel bestCompressionModel = compressionModelManager.getBestCompressionModel();

        assertEquals(dataPoints.size(), bestCompressionModel.length());
        assertEquals(TimestampCompressionModelType.REGULAR, bestCompressionModel.timestampType());
        assertEquals(ValueCompressionModelType.PMC_MEAN, bestCompressionModel.valueType());
    }
}