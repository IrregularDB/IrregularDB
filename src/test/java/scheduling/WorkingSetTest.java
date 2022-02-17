package scheduling;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import records.DataPoint;
import records.TimeSeriesReading;
import segmentgenerator.TestTimeSeries;
import segmentgenerator.TestTimeSeriesFactory;
import segmentgenerator.TimeSeries;

import javax.xml.crypto.Data;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class WorkingSetTest {

    @Test
    public void testOfWorkingSet() throws InterruptedException {
        ConcurrentLinkedQueue<TimeSeriesReading> buffer = new ConcurrentLinkedQueue<>();

        TimeSeriesReading timeSeriesReading1 = new TimeSeriesReading("key1", new DataPoint(1234567, 42.69));
        TimeSeriesReading timeSeriesReading2 = new TimeSeriesReading("key2", new DataPoint(1234, 4.69));
        TimeSeriesReading timeSeriesReading3 = new TimeSeriesReading("key1", new DataPoint(123456789, 2.69));

        TestTimeSeriesFactory testTimeSeriesFactory = new TestTimeSeriesFactory();

        WorkingSet workingSet = new WorkingSet(buffer, testTimeSeriesFactory);

        workingSet.accept(timeSeriesReading1);
        workingSet.accept(timeSeriesReading2);
        workingSet.accept(timeSeriesReading3);


        new Thread(workingSet::run).start();
        Thread.sleep(1000);


        List<TimeSeries> generatedTimeSeries = testTimeSeriesFactory.getGeneratedTimeSeries();

        Assertions.assertEquals(2, generatedTimeSeries.size());

        TestTimeSeries timeSeries1 = (TestTimeSeries)generatedTimeSeries.get(0);
        TestTimeSeries timeSeries2 = (TestTimeSeries)generatedTimeSeries.get(1);

        Assertions.assertEquals("key1", timeSeries1.getTimeSeriesKey());
        Assertions.assertEquals("key2", timeSeries2.getTimeSeriesKey());

        Assertions.assertEquals(timeSeriesReading1.dataPoint(), timeSeries1.getReceivedDataPoints().get(0));
        Assertions.assertEquals(timeSeriesReading3.dataPoint(), timeSeries1.getReceivedDataPoints().get(1));
        Assertions.assertEquals(timeSeriesReading2.dataPoint(), timeSeries2.getReceivedDataPoints().get(0));
    }

}