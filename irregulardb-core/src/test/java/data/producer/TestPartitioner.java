package data.producer;

import records.TimeSeriesReading;
import scheduling.Partitioner;
import scheduling.WorkingSet;
import storage.TestDatabaseConnectionFactory;

import java.util.Queue;

public class TestPartitioner extends Partitioner {

    private Queue<TimeSeriesReading> buffer;

    public TestPartitioner(Queue<TimeSeriesReading> buffer) {
        super(null, -1);
        this.buffer = buffer;
    }

    @Override
    public WorkingSet workingSetToSpawnReceiverFor() {
        return new WorkingSet(this.buffer, null, new TestDatabaseConnectionFactory());
    }
}
