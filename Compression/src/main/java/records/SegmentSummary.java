package records;

import java.util.List;

public class SegmentSummary {

    private final SegmentKey segmentKey;
    private final float average;

    public SegmentSummary(SegmentKey segmentKey, float average) {
        this.segmentKey = segmentKey;
        this.average = average;
    }

    public SegmentSummary(List<DataPoint> dataPointsUsedInASegment){
        this.segmentKey = null;
        average = (float)dataPointsUsedInASegment.stream().mapToDouble(DataPoint::value).average().getAsDouble();
    }

    public SegmentKey getSegmentKey() {
        return this.segmentKey;
    }

    public float getAverage() {
        return average;
    }
}
