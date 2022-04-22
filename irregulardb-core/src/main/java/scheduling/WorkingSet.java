package scheduling;

import config.ConfigProperties;
import records.FinalizeTimeSeriesReading;
import records.TimeSeriesReading;
import segmentgenerator.TimeSeries;
import segmentgenerator.TimeSeriesFactory;
import storage.DatabaseConnection;
import storage.DatabaseConnectionFactory;
import utility.Stopwatch;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public class WorkingSet {

    public static final int MAX_SIZE_OF_BUFFER_BEFORE_RECEIVER_THROTTELING = ConfigProperties.getInstance().getMaxBufferSizeBeforeThrottle();

    private final Queue<TimeSeriesReading> buffer;
    private final Map<String, TimeSeries> timeSeriesTagToTimeSeries;
    private final TimeSeriesFactory timeSeriesFactory;
    private final DatabaseConnection databaseConnection;

    public WorkingSet(Queue<TimeSeriesReading> buffer, TimeSeriesFactory timeSeriesFactory, DatabaseConnectionFactory databaseConnectionFactory) {
        this.buffer = buffer;
        this.timeSeriesTagToTimeSeries = new HashMap<>();
        this.timeSeriesFactory = timeSeriesFactory;
        this.databaseConnection = databaseConnectionFactory.createDataBaseConnection();
    }

    public int getAmtActiveTimeSeries(){
        return timeSeriesTagToTimeSeries.keySet().size(); //TODO potential multithreading problem on getting size of key set
    }

    /**
     * @param timeSeriesReading, is stored in the buffer no matter the return value
     * @return when true cantinue sending in datapoints, when false throttle data sending
     */
    public boolean accept(TimeSeriesReading timeSeriesReading){
        this.buffer.add(timeSeriesReading);
        return MAX_SIZE_OF_BUFFER_BEFORE_RECEIVER_THROTTELING > buffer.size();
    }

    public void run(){
        //TODO consider using a stop condition boolean
        while (true) {
            if (!processNextDataPoint()) {
                try { // The queue is empty so sleep for 10 ms instead of busy waiting
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean processNextDataPoint() {
        TimeSeriesReading timeSeriesReading = buffer.poll();
        if (timeSeriesReading == null) {
            return false;
        }

        String tag = timeSeriesReading.getTag();

        if (!(timeSeriesReading instanceof FinalizeTimeSeriesReading)){
            TimeSeries timeSeries = timeSeriesTagToTimeSeries.get(tag);
            if (timeSeries == null) {
                timeSeries = createTimeSeriesForNewKey(tag);
                timeSeriesTagToTimeSeries.put(tag, timeSeries);
            }
            timeSeries.processDataPoint(timeSeriesReading.getDataPoint());
        } else {
            TimeSeries timeSeriesToClose = this.timeSeriesTagToTimeSeries.get(tag);
            timeSeriesToClose.close();
            // After closing a time series we ensure all its segments are written to the DB
            databaseConnection.flushBatchToDB();
            this.timeSeriesTagToTimeSeries.remove(tag);
            Stopwatch.getDurationForTag(tag);
        }
        return true;
    }

    private TimeSeries createTimeSeriesForNewKey(String recordKey){
        return timeSeriesFactory.createTimeSeries(recordKey, this.databaseConnection);
    }
}
