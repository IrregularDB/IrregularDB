package sources;

import config.ConfigProperties;
import records.TimeSeriesReading;
import scheduling.WorkingSet;

import java.io.*;

public class CSVDataReceiver extends DataReceiver {
    private static final int AMT_TIME_TO_SLEEP = ConfigProperties.getInstance().getReceiverCSVThrottleSleepTime();

    private final String csvDelimiter;
    private final File csvFile;
    private final String fileBasedTag;

    public CSVDataReceiver(File csvFile, String fileTag, WorkingSet workingSet, String csvDelimiter) {
        super(workingSet);
        this.csvDelimiter = csvDelimiter;
        this.csvFile = csvFile;
        this.fileBasedTag = fileTag;
    }

    @Override
    public void receiveData() {
        try {
            CSVTimeSeriesReader csvTimeSeriesReader = new CSVTimeSeriesReader(this.csvFile, fileBasedTag, this.csvDelimiter);

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
