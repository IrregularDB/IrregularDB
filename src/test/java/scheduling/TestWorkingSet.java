package scheduling;

import records.TimeSeriesReading;
import segmentgenerator.TestTimeSeriesFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TestWorkingSet extends WorkingSet{
    public TestWorkingSet() {
        super(new ConcurrentLinkedQueue<>(), new TestTimeSeriesFactory());
    }

    @Override
    public void accept(TimeSeriesReading timeSeriesReading) {
        getBuffer().add(timeSeriesReading);
    }

    public Queue<TimeSeriesReading> getAcceptedRecordings() {
        return getBuffer();
    }

    @Override
    public void run() {

    }
}
