package scheduling;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class IncrementPartitionerTest {

    @Test
    void testIncrementPartitioningReturnsWorkingSetsCorrectly() {
        TestWorkingSetFactory testWorkingSetFactory = new TestWorkingSetFactory();

        IncrementPartitioner incrementPartitioner = new IncrementPartitioner(testWorkingSetFactory, 2);

        WorkingSet testWorkingSet1 = incrementPartitioner.workingSetToSpawnReceiverFor();
        WorkingSet testWorkingSet2 = incrementPartitioner.workingSetToSpawnReceiverFor();
        WorkingSet testWorkingSet3 = incrementPartitioner.workingSetToSpawnReceiverFor();

        assertNotSame(testWorkingSet1, testWorkingSet2);
        assertSame(testWorkingSet1, testWorkingSet3);
    }
}