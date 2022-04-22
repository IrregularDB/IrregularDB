package data.producer;

import records.TimeSeriesReading;

import java.io.IOException;
import java.util.List;

public class ListOfReadingsSocketProducer extends SocketProducerBase{
    private final List<TimeSeriesReading> timeSeriesReadings;

    public ListOfReadingsSocketProducer(List<TimeSeriesReading> timeSeriesReadings, String serverIp, int serverPort) throws IOException {
        super(serverIp, serverPort);
        this.timeSeriesReadings = timeSeriesReadings;
    }

    @Override
    protected void connectAndSendData(){
        try {
            for (TimeSeriesReading timeSeriesReading : timeSeriesReadings) {
                super.sendReadingToStream(timeSeriesReading);
            }
            super.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
