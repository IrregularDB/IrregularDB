package segmentgenerator;

import compression.CompressionModelFactory;
import config.ConfigProperties;
import records.DataPoint;
import records.Segment;
import records.SegmentSummary;
import storage.DatabaseConnection;

public class TimeSeries {
    private final String timeSeriesTag;
    private final DatabaseConnection databaseConnection;
    private final SegmentGenerator segmentGenerator;
    private final boolean computeSegmentSummary;

    public TimeSeries(String timeSeriesTag, DatabaseConnection dbConnection) {
        this.timeSeriesTag = timeSeriesTag;
        this.databaseConnection = dbConnection;
        int timeSeriesId = getTimeSeriesIdFromDb();
        this.segmentGenerator = new SegmentGenerator(new CompressionModelManager(CompressionModelFactory.getValueCompressionModels(), CompressionModelFactory.getTimestampCompressionModels()), timeSeriesId);
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
        Segment segment = segmentGenerator.constructSegmentFromBuffer();
        if (segment == null) {
            return false;
        } else {
            SegmentSummary segmentSummary = null;
            if (computeSegmentSummary) {
                 segmentSummary = new SegmentSummary(segment.dataPointsUsed());
            }
            sendToDb(segment, segmentSummary);
            return true;
        }
    }

    public String getTimeSeriesTag() {
        return timeSeriesTag;
    }

    private void sendToDb(Segment segment, SegmentSummary segmentSummary) {
        this.databaseConnection.insertSegment(segment, segmentSummary);
    }
}
