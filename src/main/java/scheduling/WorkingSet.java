package scheduling;

import records.TimeSeriesReading;

public interface WorkingSet {

    void accept(TimeSeriesReading timeSeriesReading);

    void run();
}
