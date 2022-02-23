package compression.utility;

import records.DataPoint;

public class LinearFunction {
    // y = ax + b
    // a: slope
    // b: intercept
    // y: value
    // x: time
    private final double slope;
    private final double intercept;

    public LinearFunction(DataPoint p1, DataPoint p2) {
        this.slope = (p2.value() - p1.value()) / (p2.timestamp() - p1.timestamp());

        this.intercept = p1.value() - this.slope * p1.timestamp();
    }

    public double getValue(Long timeStamp) {
        return this.slope * timeStamp + this.intercept;
    }

    public double getSlope() {
        return slope;
    }

    public double getIntercept() {
        return intercept;
    }
}
