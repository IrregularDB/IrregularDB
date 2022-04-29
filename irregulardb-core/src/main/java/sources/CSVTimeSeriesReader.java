package sources;

import records.DataPoint;
import records.TimeSeriesReading;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CSVTimeSeriesReader {
    private final BufferedReader bufferedReader;
    private final String csvDelimiter;
    private final String tagFromFileName;

    public CSVTimeSeriesReader(File csvFile, String csvDelimiter) throws FileNotFoundException {
        FileReader fileReader = new FileReader(csvFile);
        this.bufferedReader = new BufferedReader(fileReader);
        this.csvDelimiter = csvDelimiter;
        this.tagFromFileName = extractTagFromFileName(csvFile);
    }

    private String extractTagFromFileName(File csvFile) {
        String path = csvFile.getAbsolutePath();
        List<Integer> indexes = allIndexesOf(path, "/");
        if (indexes.isEmpty()) {
            indexes = allIndexesOf(path, "\\");
        }
        int index;
        if (indexes.size() == 1) {
            index = indexes.get(0);
        } else {
            index = indexes.get(indexes.size() - 2);
        }
        return path.substring(index, path.lastIndexOf("."));
    }

    private List<Integer> allIndexesOf(String str, String searchString) {
        List<Integer> indexes = new ArrayList<>();
        int index = 0;
        while (index != -1 && index < str.length()) {
            int currIndex = str.indexOf(searchString, index);
            if (currIndex != -1) {
                indexes.add(currIndex);
                index = currIndex +1;
            } else {
                index = currIndex;
            }
        }
        return indexes;
    }

    public void close() throws IOException {
        this.bufferedReader.close();
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
        if (split.length == 2) {
            long timestamp = Long.parseLong(split[0].trim());
            float value = Float.parseFloat(split[1].trim());
            return new TimeSeriesReading(tagFromFileName, new DataPoint(timestamp, value));
        } else {
            String tag = split[0].trim();
            long timestamp = Long.parseLong(split[1].trim());
            float value = Float.parseFloat(split[2].trim());
            return new TimeSeriesReading(tag, new DataPoint(timestamp, value));
        }
    }
}
