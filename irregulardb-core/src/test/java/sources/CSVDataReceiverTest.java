package sources;

import config.ConfigProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import records.DataPoint;
import records.FinalizeTimeSeriesReading;
import records.TimeSeriesReading;
import scheduling.WorkingSet;
import segmentgenerator.TimeSeriesFactory;
import storage.TestDatabaseConnectionFactory;

import java.io.File;
import java.util.concurrent.ConcurrentLinkedQueue;

class CSVDataReceiverTest {
    @BeforeAll
    public static void setupConfig(){
        ConfigProperties.isTest = true;
    }

    @Test
    public void csvReceiverTest(){

        TimeSeriesReading timeSeriesReadingExpected1 = new TimeSeriesReading("key1", new DataPoint(123456789, 42.69F));
        TimeSeriesReading timeSeriesReadingExpected2 = new TimeSeriesReading("key2", new DataPoint(987654321, 69.42F));

        ConcurrentLinkedQueue<TimeSeriesReading> buffer = new ConcurrentLinkedQueue<>();

        WorkingSet workingSet = new WorkingSet(buffer, new TimeSeriesFactory(), new TestDatabaseConnectionFactory());

        File file = new File("src/test/resources/testFolder/test1.csv");
        CSVDataReceiver csvDataReceiver = new CSVDataReceiver(file, workingSet, ",");

        csvDataReceiver.receiveData();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // We first get the two data points:
        Assertions.assertEquals(timeSeriesReadingExpected1, buffer.poll());
        Assertions.assertEquals(timeSeriesReadingExpected2, buffer.poll());

        // We then test that two finalized readings are gotten:
        TimeSeriesReading finalize1 = buffer.poll();
        Assertions.assertTrue(finalize1 instanceof FinalizeTimeSeriesReading);
        Assertions.assertEquals(timeSeriesReadingExpected1.getTag(), finalize1.getTag());

        TimeSeriesReading finalize2 = buffer.poll();
        Assertions.assertTrue(finalize2 instanceof FinalizeTimeSeriesReading);
        Assertions.assertEquals(timeSeriesReadingExpected2.getTag(), finalize2.getTag());

        Assertions.assertNull(buffer.poll());
    }
}