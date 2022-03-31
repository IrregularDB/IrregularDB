package segmentgenerator;

import compression.CompressionModel;
import compression.timestamp.RegularTimestampCompressionModel;
import compression.timestamp.TimestampCompressionModel;
import compression.timestamp.TimestampCompressionModelType;
import compression.value.PMCMeanValueCompressionModel;
import compression.value.ValueCompressionModel;
import compression.value.ValueCompressionModelType;
import records.DataPoint;

import java.util.ArrayList;
import java.util.List;

public class TestCompressionModelManagerRegularPMCMean extends CompressionModelManager {

    private final List<ValueCompressionModel> valueCompressionModels;
    private final List<TimestampCompressionModel> timestampCompressionModels;

    private List<DataPoint> acceptedDataPoints;

    private Float expectedValue = null;

    public TestCompressionModelManagerRegularPMCMean(List<ValueCompressionModel> valueCompressionModels, List<TimestampCompressionModel> timestampCompressionModels) {
        super(valueCompressionModels, timestampCompressionModels);
        this.valueCompressionModels = valueCompressionModels;
        this.timestampCompressionModels = timestampCompressionModels;
        this.acceptedDataPoints = new ArrayList<>();
    }

    @Override
    public boolean tryAppendDataPointToAllModels(DataPoint dataPoint) {
        if (expectedValue == null) {
            expectedValue = dataPoint.value();
            acceptedDataPoints.add(dataPoint);
            return true;
        }

        if (dataPoint.value() == expectedValue ) {
            acceptedDataPoints.add(dataPoint);
            return true;
        }
        return false;
    }

    @Override
    public boolean resetAndTryAppendBuffer(List<DataPoint> notYetEmitted) {
        this.expectedValue = null;
        this.acceptedDataPoints = new ArrayList<>();
        for (DataPoint dataPoint : notYetEmitted) {
            if (!this.tryAppendDataPointToAllModels(dataPoint)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public CompressionModel getBestCompressionModel() {
        if (timestampCompressionModels.get(0).getTimeStampCompressionModelType() == TimestampCompressionModelType.REGULAR &&
                valueCompressionModels.get(0).getValueCompressionModelType() == ValueCompressionModelType.PMC_MEAN) {

            float errorBound = 0;
            PMCMeanValueCompressionModel pmcMeanValueCompressionModel = new PMCMeanValueCompressionModel(errorBound);
            pmcMeanValueCompressionModel.resetAndAppendAll(acceptedDataPoints);

            RegularTimestampCompressionModel regularTimeStampCompressionModel = new RegularTimestampCompressionModel(0);
            regularTimeStampCompressionModel.resetAndAppendAll(acceptedDataPoints);

            return new CompressionModel(pmcMeanValueCompressionModel, regularTimeStampCompressionModel);
        }
        throw new RuntimeException("There is only a simple overwrite");
    }
}
