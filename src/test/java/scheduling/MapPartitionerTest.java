package scheduling;

import config.ConfigProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import records.DataPoint;
import records.TimeSeriesReading;

import javax.xml.crypto.Data;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class MapPartitionerTest {

    @Test
    void performPartitioning() {
        TestWorkingSetFactory testWorkingSetFactory = new TestWorkingSetFactory();

        ConfigProperties testProperties = ConfigProperties.INSTANCE;
        testProperties.setProperty("workingsets", "2");

        MapPartitioner mapPartitioner = new MapPartitioner(testWorkingSetFactory);

        WorkingSet testWorkingSet1 = mapPartitioner.workingSetToSpawnReceiverFor();
        WorkingSet testWorkingSet2 = mapPartitioner.workingSetToSpawnReceiverFor();
        WorkingSet testWorkingSet3 = mapPartitioner.workingSetToSpawnReceiverFor();

        assertNotSame(testWorkingSet1, testWorkingSet2);
        assertSame(testWorkingSet1, testWorkingSet3);
    }
}