package storage;

import records.Segment;
import records.SegmentSummary;

public interface DatabaseConnection {

    /**
     * Inserts a segment in the segment table.
     * @param segment the segment to insert
     * @param segmentSummary null is allowed
     */
    void insertSegment(Segment segment, SegmentSummary segmentSummary);

    /**
     * Gets timeSeriesId from timeSeriesTag. If not already in database
     * the time series is inserted and the generated id returned.
     * @param timeSeriesTag the time series identifier tag
     * @return time series id
     */
    int getTimeSeriesId(String timeSeriesTag);

    void flushBatchToDB();
}
