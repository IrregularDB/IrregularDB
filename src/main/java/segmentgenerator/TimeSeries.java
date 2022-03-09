package segmentgenerator;

import compression.CompressionModelFactory;
import records.DataPoint;
import records.Segment;
import storage.DatabaseConnection;

public class TimeSeries {
    private final String timeSeriesTag;
    private final DatabaseConnection databaseConnection;
    private final SegmentGenerator segmentGenerator;

    public TimeSeries(String timeSeriesTag, DatabaseConnection dbConnection) {
        this.timeSeriesTag = timeSeriesTag;
        this.databaseConnection = dbConnection;
        int timeSeriesId = getTimeSeriesIdFromDb();
        this.segmentGenerator = new SegmentGenerator(new CompressionModelManager(CompressionModelFactory.getValueCompressionModels(), CompressionModelFactory.getTimestampCompressionModels()), timeSeriesId);
    }

    private int getTimeSeriesIdFromDb() {
        return databaseConnection.getTimeSeriesId(this.timeSeriesTag);
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

    private void sendToDb(Segment segment) {
        this.databaseConnection.insertSegment(segment);
    }
}
