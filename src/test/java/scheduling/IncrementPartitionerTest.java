package scheduling;

import config.ConfigProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IncrementPartitionerTest {

    @Test
    void testIncrementPartitioningReturnsWorkingSetsCorrectly() {
        WorkingSetFactory workingSetFactory = new WorkingSetFactory();

        ConfigProperties testProperties = ConfigProperties.INSTANCE;
        testProperties.setProperty("workingsets", "2");

        IncrementPartitioner incrementPartitioner = new IncrementPartitioner(workingSetFactory);

        WorkingSet testWorkingSet1 = incrementPartitioner.workingSetToSpawnReceiverFor();
        WorkingSet testWorkingSet2 = incrementPartitioner.workingSetToSpawnReceiverFor();
        WorkingSet testWorkingSet3 = incrementPartitioner.workingSetToSpawnReceiverFor();

        assertNotSame(testWorkingSet1, testWorkingSet2);
        assertSame(testWorkingSet1, testWorkingSet3);
    }
}