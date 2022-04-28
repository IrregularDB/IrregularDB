package sources;

import records.DataPoint;
import records.TimeSeriesReading;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class CSVTimeSeriesReaderNoTags extends CSVTimeSeriesReader {
    private final String tag;

    public CSVTimeSeriesReaderNoTags(File csvFile, String csvDelimiter) throws FileNotFoundException {
        super(csvFile, csvDelimiter);
        String path = csvFile.getAbsolutePath();
        tag = path.substring(path.indexOf("house"), path.indexOf(".csv"));
    }

    @Override
    public TimeSeriesReading next() throws IOException {
        String line = bufferedReader.readLine();
        if (line == null) {
            return null; //end of file
        }
        return parseLine(line);
    }

    private TimeSeriesReading parseLine(String line) {
        String[] split = line.split(this.csvDelimiter);

        long timestamp = Long.parseLong(split[0].trim());
        float value = Float.parseFloat(split[1].trim());
        return new TimeSeriesReading(tag, new DataPoint(timestamp, value));
    }

}
