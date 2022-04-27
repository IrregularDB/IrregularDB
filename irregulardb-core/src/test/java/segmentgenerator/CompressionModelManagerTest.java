package segmentgenerator;

import compression.timestamp.DeltaDeltaTimestampCompressionModel;
import compression.timestamp.RegularTimestampCompressionModel;
import compression.timestamp.SIDiffTimestampCompressionModel;
import compression.timestamp.TimestampCompressionModel;
import compression.value.GorillaValueCompressionModel;
import compression.value.PMCMeanValueCompressionModel;
import compression.value.SwingValueCompressionModel;
import compression.value.ValueCompressionModel;
import config.ConfigProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import records.DataPoint;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CompressionModelManagerTest {
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
    }

    @Test
    void getBestCompressionModel() {
    }
}