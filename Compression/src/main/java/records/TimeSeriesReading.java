package records;

import java.util.Objects;

public class TimeSeriesReading {
    private final String tag;
    private final DataPoint dataPoint;

    public TimeSeriesReading(String tag, DataPoint dataPoint) {
        this.tag = tag;
        this.dataPoint = dataPoint;
    }

    public String getTag() {
        return tag;
    }

    public DataPoint getDataPoint() {
        return dataPoint;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TimeSeriesReading that = (TimeSeriesReading) o;
        return Objects.equals(tag, that.tag) && Objects.equals(dataPoint, that.dataPoint);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tag, dataPoint);
    }
}

