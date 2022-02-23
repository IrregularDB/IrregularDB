package segmentgenerator;

import compression.CompressionModelFactory;
import records.DataPoint;
import records.Segment;

public class TimeSeries {

    private final String timeSeriesKey;
    private final SegmentGenerator segmentGenerator;

    public TimeSeries(String timeSeriesKey) {
        this.timeSeriesKey = timeSeriesKey;
        this.segmentGenerator = new SegmentGenerator(new CompressionModelManager(CompressionModelFactory.getValueCompressionModels(), CompressionModelFactory.getTimestampCompressionModels()), timeSeriesKey);
    }

    public void processDataPoint(DataPoint dataPoint) {
        if (!segmentGenerator.acceptDataPoint(dataPoint)) {
            Segment segment = segmentGenerator.constructSegmentFromBuffer();
            sendToDb(segment);
        }
    }

    public String getTimeSeriesKey() {
        return timeSeriesKey;
    }

    void sendToDb(Segment segment) {
        System.out.println(segment + "sent to db");
    }
}
