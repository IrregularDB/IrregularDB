package sources;

import config.ConfigProperties;
import records.Pair;
import records.TimeSeriesReading;
import scheduling.WorkingSet;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class CSVListDataReceiver extends DataReceiver{
    private static final int AMT_TIME_TO_SLEEP = ConfigProperties.getInstance().getReceiverCSVThrottleSleepTime();

    private List<Pair<File, String>> fileAndTags;
    private String csvDelimiter;

    public CSVListDataReceiver(List<Pair<File,String>> fileAndFileTag, WorkingSet workingSet, String csvDelimiter) {
        super(workingSet);
        this.fileAndTags = fileAndFileTag;
        this.csvDelimiter = csvDelimiter;
    }

    @Override
    public void receiveData() {
        for (Pair<File, String> fileAndTag : fileAndTags) {
            try {
                CSVTimeSeriesReader csvTimeSeriesReader = new CSVTimeSeriesReader(fileAndTag.f0(), fileAndTag.f1(), this.csvDelimiter);

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
}
