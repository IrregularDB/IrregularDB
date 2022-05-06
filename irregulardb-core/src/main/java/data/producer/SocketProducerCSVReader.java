package data.producer;

import records.TimeSeriesReading;
import scheduling.WorkingSet;
import sources.CSVTimeSeriesReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

public class SocketProducerCSVReader extends SocketProducerBase {
    private final File csvFile;
    private final String csvDelimiter;
    private final String fallbackTag;

    public SocketProducerCSVReader(File csvFile,String fallbackTag, String csvDelimiter, String serverIp, int serverPort) throws IOException {
        super(serverIp, serverPort);
        this.csvFile = csvFile;
        this.csvDelimiter = csvDelimiter;
        this.fallbackTag = fallbackTag;
    }

    @Override
    protected void connectAndSendData() {
        try {
            CSVTimeSeriesReader csvTimeSeriesReader = new CSVTimeSeriesReader(this.csvFile, this.fallbackTag, this.csvDelimiter);
            TimeSeriesReading timeSeriesReading = csvTimeSeriesReader.next();
            while (timeSeriesReading != null) {
                super.sendReadingToStream(timeSeriesReading);
                timeSeriesReading = csvTimeSeriesReader.next();
            }
            System.out.println("Socket producer done with: " + this.csvFile.getAbsolutePath());
            csvTimeSeriesReader.close();
            super.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
