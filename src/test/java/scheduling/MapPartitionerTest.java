package scheduling;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import records.DataPoint;
import records.TimeSeriesReading;

import javax.xml.crypto.Data;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class MapPartitionerTest {

    @Test
    void performPartitioning() {
        TestWorkingSetFactory testWorkingSetFactory = new TestWorkingSetFactory();

        MapPartitioner mapPartitioner = new MapPartitioner(testWorkingSetFactory, 2);

        WorkingSet testWorkingSet1 = mapPartitioner.workingSetToSpawnReceiverFor();
        WorkingSet testWorkingSet2 = mapPartitioner.workingSetToSpawnReceiverFor();
        WorkingSet testWorkingSet3 = mapPartitioner.workingSetToSpawnReceiverFor();

        assertNotSame(testWorkingSet1, testWorkingSet2);
        assertSame(testWorkingSet1, testWorkingSet3);
    }
}