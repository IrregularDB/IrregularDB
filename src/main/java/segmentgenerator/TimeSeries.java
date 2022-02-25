package segmentgenerator;

import compression.CompressionModelFactory;
import records.DataPoint;
import records.Segment;
import storage.DatabaseConnection;
import storage.PostgresConnection;

public class TimeSeries {
    private final String timeSeriesTag;
    private int timeSeriesId;
    private final DatabaseConnection databaseConnection;
    private final SegmentGenerator segmentGenerator;

    public TimeSeries(String timeSeriesTag, DatabaseConnection dbConnection) {
        this.timeSeriesTag = timeSeriesTag;
        this.databaseConnection = dbConnection;
        this.segmentGenerator = new SegmentGenerator(new CompressionModelManager(CompressionModelFactory.getValueCompressionModels(), CompressionModelFactory.getTimestampCompressionModels()), timeSeriesId);
    }

    public void processDataPoint(DataPoint dataPoint) {
        this.timeSeriesId = databaseConnection.getTimeSeriesId(this.timeSeriesTag);

        if (!segmentGenerator.acceptDataPoint(dataPoint)) {
            Segment segment = segmentGenerator.constructSegmentFromBuffer();
            sendToDb(segment);
        }
    }

    public String getTimeSeriesTag() {
        return timeSeriesTag;
    }

    void sendToDb(Segment segment) {
        this.databaseConnection.insertSegment(segment);
    }
}
