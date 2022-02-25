package segmentgenerator;

import storage.DatabaseConnection;

public class TimeSeriesFactory {

    public  TimeSeries createTimeSeries(String timeSeriesKey, DatabaseConnection dbConnection){
        return new TimeSeries(timeSeriesKey, dbConnection);
    }
}
