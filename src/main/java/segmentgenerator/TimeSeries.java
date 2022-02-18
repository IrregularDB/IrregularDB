package segmentgenerator;

import compression.CompressionModelFactory;
import records.Segment;
import records.DataPoint;

import java.util.Optional;

public class TimeSeries {

    private final String timeSeriesKey;
    private final SegmentGenerator segmentGenerator;

    public TimeSeries(String timeSeriesKey) {
        this.timeSeriesKey = timeSeriesKey;
        this.segmentGenerator = new SegmentGenerator(CompressionModelFactory.getValueCompressionModels(), CompressionModelFactory.getTimestampCompressionModels(), timeSeriesKey);
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
