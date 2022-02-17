package sources;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import records.DataPoint;
import records.TimeSeriesReading;
import scheduling.TestWorkingSet;

import java.util.concurrent.ConcurrentLinkedQueue;

class CSVDataReceiverTest {

    @Test
    public void csvReceiverTest(){

        TimeSeriesReading timeSeriesReadingExpected1 = new TimeSeriesReading("key1", new DataPoint(123456789, 42.69));
        TimeSeriesReading timeSeriesReadingExpected2 = new TimeSeriesReading("key2", new DataPoint(987654321, 69.42));

        TestWorkingSet testWorkingSet = new TestWorkingSet();

        CSVDataReceiver csvDataReceiver = new CSVDataReceiver("src/test/resources/test.csv", testWorkingSet, ",");

        csvDataReceiver.receiveData();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assertions.assertEquals(timeSeriesReadingExpected1, testWorkingSet.getBuffer().poll());
        Assertions.assertEquals(timeSeriesReadingExpected2, testWorkingSet.getBuffer().poll());
    }

}