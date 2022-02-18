package segmentgenerator;

import records.DataPoint;

public class TimeSeries {

    private final String timeSeriesKey;

    public TimeSeries(String timeSeriesKey) {
        this.timeSeriesKey = timeSeriesKey;
    }

    public void processDataPoint(DataPoint dataPoint) {
        Optional<Segment> segment = segmentGenerator.acceptDataPoint(dataPoint);
        segment.ifPresent(this::sendToDb);
    }

    public String getTimeSeriesKey() {
        return timeSeriesKey;
    }

    void sendToDb(Segment segment){
        throw new RuntimeException();
    }
}
