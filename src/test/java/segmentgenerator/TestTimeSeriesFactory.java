package segmentgenerator;

import java.util.ArrayList;
import java.util.List;

public class TestTimeSeriesFactory extends TimeSeriesFactory {

    private List<TimeSeries> generatedTimeSeries;

    public TestTimeSeriesFactory() {
        this.generatedTimeSeries = new ArrayList<>();
    }

    @Override
    public TimeSeries createTimeSeries(String timeSeriesKey) {
        TestTimeSeries testTimeSeries = new TestTimeSeries(timeSeriesKey);
        this.generatedTimeSeries.add(testTimeSeries);
        return testTimeSeries;
    }

    public List<TimeSeries> getGeneratedTimeSeries() {
        return generatedTimeSeries;
    }
}
