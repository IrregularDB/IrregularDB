package segmentgenerator;

import records.DataPoint;
import storage.DatabaseConnection;

import java.util.ArrayList;
import java.util.List;

public class TestTimeSeries extends TimeSeries{
    private final List<DataPoint> receivedDataPoints;

    public TestTimeSeries(String timeSeriesKey) {
        super(timeSeriesKey);
        this.receivedDataPoints = new ArrayList<>();
    }

    @Override
    public void processDataPoint(DataPoint dataPoint) {
        receivedDataPoints.add(dataPoint);
    }

    public List<DataPoint> getReceivedDataPoints() {
        return receivedDataPoints;
    }
}
