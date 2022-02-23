package segmentgenerator;

import compression.CompressionModelFactory;
import records.DataPoint;
import records.Segment;
import storage.DatabaseConnection;
import storage.PostgresConnection;

public class TimeSeries {
    private final String timeSeriesTag;
    private final int timeSeriesId;
    private final DatabaseConnection databaseConnection;
    private final SegmentGenerator segmentGenerator;

    public TimeSeries(String timeSeriesKey) {
        this.timeSeriesTag = timeSeriesKey;
        this.databaseConnection = new PostgresConnection();
        this.timeSeriesId = databaseConnection.getTimeSeriesId(this.timeSeriesTag);
        this.segmentGenerator = new SegmentGenerator(new CompressionModelManager(CompressionModelFactory.getValueCompressionModels(), CompressionModelFactory.getTimestampCompressionModels()), timeSeriesId);
    }

    public void processDataPoint(DataPoint dataPoint) {
        if (!segmentGenerator.acceptDataPoint(dataPoint)) {
            Segment segment = segmentGenerator.constructSegmentFromBuffer();
            sendToDb(segment);
        }
    }

    public String getTimeSeriesTag() {
        return timeSeriesTag;
    }

    void sendToDb(Segment segment) {
        System.out.println(segment + "sent to db");
    }
}
