package segmentgenerator;

import records.DataPoint;

public class TimeSeries {

    private final String timeSeriesKey;

    public TimeSeries(String timeSeriesKey) {
        this.timeSeriesKey = timeSeriesKey;
    }

    public void processDataPoint(DataPoint dataPoint) {

    }

    public String getTimeSeriesKey() {
        return timeSeriesKey;
    }
}
