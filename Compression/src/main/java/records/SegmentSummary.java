package records;

import java.util.List;
import java.util.Objects;

public class SegmentSummary {
    private final SegmentKey segmentKey;
    private final float minValue;
    private final float maxValue;
    private final int amtDataPoints;

    public SegmentSummary(List<DataPoint> dataPointsUsedInASegment, SegmentKey segmentKey){
        this.segmentKey = segmentKey;

        this.minValue = (float)dataPointsUsedInASegment.stream()
                            .mapToDouble(DataPoint::value)
                            .min()
                            .getAsDouble();

        this.maxValue = (float)dataPointsUsedInASegment.stream()
                        .mapToDouble(DataPoint::value)
                        .max()
                        .getAsDouble();

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SegmentSummary that = (SegmentSummary) o;
        return Float.compare(that.getMinValue(), getMinValue()) == 0 && Float.compare(that.getMaxValue(), getMaxValue()) == 0 && getAmtDataPoints() == that.getAmtDataPoints() && getSegmentKey().equals(that.getSegmentKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSegmentKey(), getMinValue(), getMaxValue(), getAmtDataPoints());
    }
}
