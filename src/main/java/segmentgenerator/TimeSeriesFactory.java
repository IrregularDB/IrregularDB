package segmentgenerator;

public class TimeSeriesFactory {

    public TimeSeries createTimeSeries(String timeSeriesKey){
        return new TimeSeries(timeSeriesKey);
    }
}
