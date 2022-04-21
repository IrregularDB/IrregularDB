package scheduling;

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

    public void accept(TimeSeriesReading timeSeriesReading){
        this.buffer.add(timeSeriesReading);
    }

    public void run(){
        //TODO consider using a stop condition boolean
        while (true) {
            if (!processNextDataPoint()) {
                try {
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

        if (timeSeriesReading instanceof FinalizeTimeSeriesReading){
            TimeSeries timeSeriesToClose = this.timeSeriesTagToTimeSeries.get(tag);
            timeSeriesToClose.close();
            this.timeSeriesTagToTimeSeries.remove(tag);
            Stopwatch.getDurationForTag(tag);
        } else{
            TimeSeries timeSeries = timeSeriesTagToTimeSeries.get(tag);
            if (timeSeries == null) {
                timeSeries = createTimeSeriesForNewKey(tag);
                timeSeriesTagToTimeSeries.put(tag, timeSeries);
            }
            timeSeries.processDataPoint(timeSeriesReading.getDataPoint());
        }

        return true;
    }

    private TimeSeries createTimeSeriesForNewKey(String recordKey){
        return timeSeriesFactory.createTimeSeries(recordKey, this.databaseConnection);
    }
}
