package scheduling;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import records.DataPoint;
import records.TimeSeriesReading;

import javax.xml.crypto.Data;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class MapPartitionerTest {

    @Test
    void performPartitioning() {
        TestWorkingSetFactory testWorkingSetFactory = new TestWorkingSetFactory();
        ConcurrentLinkedQueue<TimeSeriesReading> mainBuffer = new ConcurrentLinkedQueue<>();

        MapPartitioner mapPartitioner = new MapPartitioner(testWorkingSetFactory, mainBuffer);

        TimeSeriesReading timeSeriesReading1 = new TimeSeriesReading("key1", new DataPoint(123, 1.1));
        TimeSeriesReading timeSeriesReading2 = new TimeSeriesReading("key2", new DataPoint(123, 1.3));
        TimeSeriesReading timeSeriesReading3 = new TimeSeriesReading("key1", new DataPoint(124, 1.2));

        mainBuffer.add(timeSeriesReading1);
        mainBuffer.add(timeSeriesReading2);
        mainBuffer.add(timeSeriesReading3);

        new Thread(mapPartitioner::performPartitioning).start();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<TestWorkingSetFactory.TestWorkingSet> generatedWorkingSets = testWorkingSetFactory.getGeneratedWorkingSets();
        TestWorkingSetFactory.TestWorkingSet testWorkingSet1 = generatedWorkingSets.get(0);
        TestWorkingSetFactory.TestWorkingSet testWorkingSet2 = generatedWorkingSets.get(1);

        List<TimeSeriesReading> acceptedRecordings1 = testWorkingSet1.getAcceptedRecordings();
        List<TimeSeriesReading> acceptedRecordings2 = testWorkingSet2.getAcceptedRecordings();

        Assertions.assertEquals(timeSeriesReading1, acceptedRecordings1.get(0));
        Assertions.assertEquals(timeSeriesReading3, acceptedRecordings1.get(1));

        Assertions.assertEquals(timeSeriesReading2, acceptedRecordings2.get(0));
    }
}