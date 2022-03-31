package scheduling;

import config.ConfigProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import records.DataPoint;
import records.TimeSeriesReading;
import segmentgenerator.TestTimeSeries;
import segmentgenerator.TestTimeSeriesFactory;
import segmentgenerator.TimeSeries;
import storage.TestDatabaseConnectionFactory;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

class WorkingSetTest {
    @BeforeAll
    public static void setupConfig(){
        ConfigProperties.isTest = true;
    }

    @Test
    public void testOfWorkingSet() throws InterruptedException {
        ConcurrentLinkedQueue<TimeSeriesReading> buffer = new ConcurrentLinkedQueue<>();

        TimeSeriesReading timeSeriesReading1 = new TimeSeriesReading("key1", new DataPoint(1234567, 42.69F));
        TimeSeriesReading timeSeriesReading2 = new TimeSeriesReading("key2", new DataPoint(1234, 4.69F));
        TimeSeriesReading timeSeriesReading3 = new TimeSeriesReading("key1", new DataPoint(123456789, 2.69F));

        TestTimeSeriesFactory testTimeSeriesFactory = new TestTimeSeriesFactory();

        WorkingSet workingSet = new WorkingSet(buffer, testTimeSeriesFactory, new TestDatabaseConnectionFactory());

        workingSet.accept(timeSeriesReading1);
        workingSet.accept(timeSeriesReading2);
        workingSet.accept(timeSeriesReading3);


        new Thread(workingSet::run).start();
        Thread.sleep(1000);


        List<TimeSeries> generatedTimeSeries = testTimeSeriesFactory.getGeneratedTimeSeries();

        Assertions.assertEquals(2, generatedTimeSeries.size());

        TestTimeSeries timeSeries1 = (TestTimeSeries)generatedTimeSeries.get(0);
        TestTimeSeries timeSeries2 = (TestTimeSeries)generatedTimeSeries.get(1);

        Assertions.assertEquals("key1", timeSeries1.getTimeSeriesTag());
        Assertions.assertEquals("key2", timeSeries2.getTimeSeriesTag());

        Assertions.assertEquals(timeSeriesReading1.getDataPoint(), timeSeries1.getReceivedDataPoints().get(0));
        Assertions.assertEquals(timeSeriesReading3.getDataPoint(), timeSeries1.getReceivedDataPoints().get(1));
        Assertions.assertEquals(timeSeriesReading2.getDataPoint(), timeSeries2.getReceivedDataPoints().get(0));
    }

}