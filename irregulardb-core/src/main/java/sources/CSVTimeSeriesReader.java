package sources;

import records.DataPoint;
import records.TimeSeriesReading;

import java.io.*;

public class CSVTimeSeriesReader {
    private final BufferedReader bufferedReader;
    private final String csvDelimiter;

    public CSVTimeSeriesReader(File csvFile, String csvDelimiter) throws FileNotFoundException {
        FileReader fileReader = new FileReader(csvFile);
        this.bufferedReader = new BufferedReader(fileReader);

        this.csvDelimiter = csvDelimiter;
    }

    /**
     * @return null if end of file
     */
    public TimeSeriesReading next() throws IOException {
        String line = bufferedReader.readLine();
        if (line == null) {
            return null; //end of file
        }
        return parseLine(line);
    }


    private TimeSeriesReading parseLine(String line) {
        String[] split = line.split(this.csvDelimiter);

        String id = split[0].trim();
        long timestamp = Long.parseLong(split[1].trim());
        float value = Float.parseFloat(split[2].trim());

        return new TimeSeriesReading(id, new DataPoint(timestamp, value));
    }
}
