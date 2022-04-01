package records;

import java.util.List;

public class SegmentSummary {

    private final SegmentKey segmentKey;
    private final float minValue;
    private final float maxValue;

    public SegmentSummary(SegmentKey segmentKey, float minValue, float maxValue) {
        this.segmentKey = segmentKey;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    public SegmentSummary(List<DataPoint> dataPointsUsedInASegment){
        this.segmentKey = null;

        this.minValue = Math.nextDown(
                    (float)dataPointsUsedInASegment.stream()
                            .mapToDouble(DataPoint::value)
                            .min()
                            .getAsDouble()
        );

        this.maxValue = Math.nextUp(
                (float)dataPointsUsedInASegment.stream()
                        .mapToDouble(DataPoint::value)
                        .max()
                        .getAsDouble()
        );
    }

    public SegmentKey getSegmentKey() {
        return this.segmentKey;
    }

    public float getMinValue() {
        return minValue;
    }

    public float getMaxValue() {
        return maxValue;
    }
}
