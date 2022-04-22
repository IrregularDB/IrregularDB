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
    private final static int BATCH_SIZE = 1000000;

    public SocketProducerCSVReader(File csvFile, String csvDelimiter, String serverIp, int serverPort) throws IOException {
        super(serverIp, serverPort);
        this.csvFile = csvFile;
        this.csvDelimiter = csvDelimiter;
    }

    @Override
    protected void connectAndSendData() {
        try {
            CSVTimeSeriesReader csvTimeSeriesReader = new CSVTimeSeriesReader(this.csvFile, this.csvDelimiter);
            List<TimeSeriesReading> buffer = new ArrayList<>();
            TimeSeriesReading timeSeriesReading = csvTimeSeriesReader.next();
            while (timeSeriesReading != null) {
                buffer.add(timeSeriesReading);
                if (buffer.size() == BATCH_SIZE) {
                    //flushBuffer(buffer);
                }
                timeSeriesReading = csvTimeSeriesReader.next();
            }
            System.out.println("Socket producer has read: " + this.csvFile.getAbsolutePath());
            flushBuffer(buffer); // TODO: everything seems to point to the socket connection and writing over the socket being slow
            System.out.println("Socket producer done with: " + this.csvFile.getAbsolutePath());
            super.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void flushBuffer(List<TimeSeriesReading> buffer) throws IOException {
        for (TimeSeriesReading seriesReading : buffer) {
            super.sendReadingToStream(seriesReading);
        }
        buffer.clear();
    }
}
