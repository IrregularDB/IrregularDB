package segmentgenerator;

import compression.CompressionModel;
import compression.timestamp.RegularTimeStampCompressionModel;
import compression.timestamp.TimeStampCompressionModel;
import compression.timestamp.TimeStampCompressionModelType;
import compression.value.PMCMeanValueCompressionModel;
import compression.value.ValueCompressionModel;
import compression.value.ValueCompressionModelType;
import records.DataPoint;

import java.util.ArrayList;
import java.util.List;

public class TestCompressionModelManagerRegularPMCMean extends CompressionModelManager {

    private final List<ValueCompressionModel> valueCompressionModels;
    private final List<TimeStampCompressionModel> timeStampCompressionModels;

    private List<DataPoint> acceptedDataPoints;

    private Float expectedValue = null;

    public TestCompressionModelManagerRegularPMCMean(List<ValueCompressionModel> valueCompressionModels, List<TimeStampCompressionModel> timeStampCompressionModels) {
        super(valueCompressionModels, timeStampCompressionModels);
        this.valueCompressionModels = valueCompressionModels;
        this.timeStampCompressionModels = timeStampCompressionModels;
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
        if (timeStampCompressionModels.get(0).getTimeStampCompressionModelType() == TimeStampCompressionModelType.REGULAR &&
                valueCompressionModels.get(0).getValueCompressionModelType() == ValueCompressionModelType.PMC_MEAN) {

            float errorBound = 0;
            PMCMeanValueCompressionModel pmcMeanValueCompressionModel = new PMCMeanValueCompressionModel(errorBound);
            pmcMeanValueCompressionModel.resetAndAppendAll(acceptedDataPoints);

            RegularTimeStampCompressionModel regularTimeStampCompressionModel = new RegularTimeStampCompressionModel(0);
            regularTimeStampCompressionModel.resetAndAppendAll(acceptedDataPoints);

            return new CompressionModel(pmcMeanValueCompressionModel, regularTimeStampCompressionModel);
        }
        throw new RuntimeException("There is only a simple overwrite");
    }
}
