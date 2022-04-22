package segmentgenerator;

import compression.CompressionModelFactory;
import config.ConfigProperties;
import records.DataPoint;
import records.Segment;
import records.SegmentSummary;
import storage.DatabaseConnection;

import java.util.List;

public class TimeSeries {
    private final String timeSeriesTag;
    private final DatabaseConnection databaseConnection;
    private final SegmentGenerator segmentGenerator;
    private final boolean computeSegmentSummary;

    public TimeSeries(String timeSeriesTag, DatabaseConnection dbConnection) {
        this.timeSeriesTag = timeSeriesTag;
        this.databaseConnection = dbConnection;
        int timeSeriesId = getTimeSeriesIdFromDb();
        this.segmentGenerator = new SegmentGenerator(new CompressionModelManager(CompressionModelFactory.getValueCompressionModels(timeSeriesTag), CompressionModelFactory.getTimestampCompressionModels(timeSeriesTag)), timeSeriesId);
        this.computeSegmentSummary = ConfigProperties.getInstance().populateSegmentSummary();
    }

    private int getTimeSeriesIdFromDb() {
        return databaseConnection.getTimeSeriesId(this.timeSeriesTag);
    }

    public void processDataPoint(DataPoint dataPoint) {
        if (!segmentGenerator.acceptDataPoint(dataPoint)) {
            getSegmentAndSendToDB();
        }
    }

    public void close(){
        while (getSegmentAndSendToDB()) {}
    }

    private boolean getSegmentAndSendToDB(){
        List<Segment> segments = segmentGenerator.constructSegmentsFromBuffer();
        if (segments == null) {
            return false;
        } else {
            for (Segment segment : segments) {
                this.databaseConnection.insertSegment(segment);
            }
            return true;
        }
    }

    public String getTimeSeriesTag() {
        return timeSeriesTag;
    }
}
