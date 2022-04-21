package records;

import java.util.List;

public class SegmentSummary {

    private final SegmentKey segmentKey;
    private final float minValue;
    private final float maxValue;
    private final int amtDataPoints;

    public SegmentSummary(SegmentKey segmentKey, float minValue, float maxValue,int amtDataPoints) {
        this.segmentKey = segmentKey;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.amtDataPoints = amtDataPoints;
    }

    public SegmentSummary(List<DataPoint> dataPointsUsedInASegment){
        this.segmentKey = null;

        this.minValue = (float)dataPointsUsedInASegment.stream()
                            .mapToDouble(DataPoint::value)
                            .min()
                            .getAsDouble()
        ;

        this.maxValue = (float)dataPointsUsedInASegment.stream()
                        .mapToDouble(DataPoint::value)
                        .max()
                        .getAsDouble()
        ;

        this.amtDataPoints = dataPointsUsedInASegment.size();
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

    public int getAmtDataPoints() {
        return amtDataPoints;
    }
}
