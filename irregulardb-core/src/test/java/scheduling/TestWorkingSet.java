package scheduling;

import records.TimeSeriesReading;
import segmentgenerator.TimeSeriesFactory;
import storage.DatabaseConnectionFactory;

import java.util.Queue;

public class TestWorkingSet extends WorkingSet{

    public TestWorkingSet(Queue<TimeSeriesReading> buffer, TimeSeriesFactory timeSeriesFactory, DatabaseConnectionFactory databaseConnectionFactory) {
        super(buffer, timeSeriesFactory, databaseConnectionFactory);
    }

    @Override
    public void accept(TimeSeriesReading timeSeriesReading) {
    }

    @Override
    public void run() {
    }
}
