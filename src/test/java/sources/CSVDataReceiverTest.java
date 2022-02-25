package sources;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import records.DataPoint;
import records.TimeSeriesReading;
import scheduling.WorkingSet;
import segmentgenerator.TestTimeSeriesFactory;
import segmentgenerator.TimeSeriesFactory;
import storage.DatabaseConnectionFactory;
import storage.TestDatabaseConnectionFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

class CSVDataReceiverTest {

    @Test
    public void csvReceiverTest(){

        TimeSeriesReading timeSeriesReadingExpected1 = new TimeSeriesReading("key1", new DataPoint(123456789, 42.69F));
        TimeSeriesReading timeSeriesReadingExpected2 = new TimeSeriesReading("key2", new DataPoint(987654321, 69.42F));

        ConcurrentLinkedQueue<TimeSeriesReading> buffer = new ConcurrentLinkedQueue<>();

        WorkingSet workingSet = new WorkingSet(buffer, new TimeSeriesFactory(), new TestDatabaseConnectionFactory());

        CSVDataReceiver csvDataReceiver = new CSVDataReceiver("src/test/resources/test.csv", workingSet, ",");

        csvDataReceiver.receiveData();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assertions.assertEquals(timeSeriesReadingExpected1, buffer.poll());
        Assertions.assertEquals(timeSeriesReadingExpected2, buffer.poll());
        // Hehe
        Assertions.assertNull(buffer.poll());
    }

}