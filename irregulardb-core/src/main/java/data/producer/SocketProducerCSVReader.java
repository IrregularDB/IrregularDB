package data.producer;

import records.TimeSeriesReading;
import sources.CSVTimeSeriesReader;
import java.io.IOException;

public class SocketProducerCSVReader extends SocketProducerBase {
    private CSVTimeSeriesReader csvTimeSeriesReader;

    public SocketProducerCSVReader(CSVTimeSeriesReader csvTimeSeriesReader, String serverIp, int serverPort) throws IOException {
        super(serverIp, serverPort);
        this.csvTimeSeriesReader = csvTimeSeriesReader;
    }


    @Override
    protected void connectAndSendData() {
        try {
            TimeSeriesReading timeSeriesReading = csvTimeSeriesReader.next();
            while (timeSeriesReading != null) {
                super.sendReadingToStream(timeSeriesReading);
                timeSeriesReading = csvTimeSeriesReader.next();
            }
            super.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
