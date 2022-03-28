package segmentgenerator;

import storage.DatabaseConnection;

import java.util.ArrayList;
import java.util.List;

public class TestTimeSeriesFactory extends TimeSeriesFactory {

    private final List<TimeSeries> generatedTimeSeries;

    public TestTimeSeriesFactory() {
        this.generatedTimeSeries = new ArrayList<>();
    }

    @Override
    public TimeSeries createTimeSeries(String timeSeriesKey, DatabaseConnection dbConnection) {
        TestTimeSeries testTimeSeries = new TestTimeSeries(timeSeriesKey, dbConnection);
        this.generatedTimeSeries.add(testTimeSeries);
        return testTimeSeries;
    }

    public List<TimeSeries> getGeneratedTimeSeries() {
        return generatedTimeSeries;
    }
}
