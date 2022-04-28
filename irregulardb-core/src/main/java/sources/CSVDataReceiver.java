package sources;

import config.ConfigProperties;
import records.TimeSeriesReading;
import scheduling.WorkingSet;

import java.io.*;

public class CSVDataReceiver extends DataReceiver {
    private final String csvDelimiter;
    private final File csvFile;
    private static final int AMT_TIME_TO_SLEEP = ConfigProperties.getInstance().getReceiverCSVThrottleSleepTime();

    public CSVDataReceiver(File csvFile, WorkingSet workingSet, String csvDelimiter) {
        super(workingSet);
        this.csvDelimiter = csvDelimiter;
        this.csvFile = csvFile;
    }

    @Override
    public void receiveData() {
        try {
            CSVTimeSeriesReader csvTimeSeriesReader = new CSVTimeSeriesReaderNoTags(this.csvFile, this.csvDelimiter);

            TimeSeriesReading timeSeriesReading = csvTimeSeriesReader.next();
            while (timeSeriesReading != null) {
                if (!sendTimeSeriesReadingToBuffer(timeSeriesReading)) {
                        Thread.sleep(AMT_TIME_TO_SLEEP);
                }
                timeSeriesReading = csvTimeSeriesReader.next();
            }

            csvTimeSeriesReader.close();
            super.close();
        } catch (IOException | InterruptedException e){
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}
