package sources;

import records.DataPoint;
import records.TimeSeriesReading;
import scheduling.WorkingSet;

import java.io.*;

public class CSVDataReceiver extends DataReceiver {

    private final String elementDelimiter;
    private final File csvFile;

    public CSVDataReceiver(File csvFile, WorkingSet workingSet, String elementDelimiter) {
        super(workingSet);
        this.elementDelimiter = elementDelimiter;
        this.csvFile = csvFile;
    }

    @Override
    public void receiveData() {
        try {
            FileReader fileReader = new FileReader(csvFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            readFile(bufferedReader);

            super.close();
            System.out.println("CSVReceiver for " + csvFile.getAbsolutePath() + " has completed delivering its data");
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
        long timestamp = Long.parseLong(split[1].trim());
        float value = Float.parseFloat(split[2].trim());

        return new TimeSeriesReading(id, new DataPoint(timestamp, value));
    }
}
