package segmentgenerator;

import compression.CompressionModelFactory;
import records.DataPoint;
import records.Segment;
import storage.DatabaseConnection;

import java.util.List;

public class TimeSeries {
    private final String timeSeriesTag;
    private final DatabaseConnection databaseConnection;
    private final SegmentGenerator segmentGenerator;

    public TimeSeries(String timeSeriesTag, DatabaseConnection dbConnection) {
        this.timeSeriesTag = timeSeriesTag;
        this.databaseConnection = dbConnection;
        int timeSeriesId = databaseConnection.getTimeSeriesId(timeSeriesTag);
        CompressionModelManager compressionModelManager = new CompressionModelManager(CompressionModelFactory.getValueCompressionModels(timeSeriesTag), CompressionModelFactory.getTimestampCompressionModels(timeSeriesTag), CompressionModelFactory.getModelPickerType());
        this.segmentGenerator = new SegmentGenerator(compressionModelManager, timeSeriesId);
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
