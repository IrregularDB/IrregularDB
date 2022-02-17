package sources;

import records.DataPoint;
import records.TimeSeriesReading;
import scheduling.WorkingSet;

import java.io.*;
import java.util.Queue;

public class CSVDataReceiver extends DataReceiver {

    private final String elementDelimiter;
    private final String absoluteFilePath;


    public CSVDataReceiver(String absoluteFilePath, WorkingSet systemTimeSeries, String elementDelimiter) {
        super(systemTimeSeries);
        this.elementDelimiter = elementDelimiter;
        this.absoluteFilePath = absoluteFilePath;
    }

    @Override
    public void receiveData() {
        File file = new File(absoluteFilePath);
        FileReader fileReader;
        try {
            fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            readFile(bufferedReader);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readFile(BufferedReader bufferedReader) throws IOException {
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            TimeSeriesReading timeSeriesReading = parseLine(line);
            sendTimeSeriesReadingToBuffer(timeSeriesReading);
        }
    }

    private TimeSeriesReading parseLine(String line) {
        String[] split = line.split(this.elementDelimiter);

        String id = split[0].trim();
        long timeStamp = Long.parseLong(split[1].trim());
        double value = Double.parseDouble(split[2].trim());

        return new TimeSeriesReading(id, new DataPoint(timeStamp, value));
    }
}
