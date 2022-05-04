package debugging;


import compression.BlobDecompressor;
import compression.timestamp.DeltaDeltaTimestampCompressionModel;
import compression.timestamp.RegularTimestampCompressionModel;
import compression.timestamp.TimestampCompressionModel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import records.DataPoint;
import records.TimeSeriesReading;
import sources.CSVTimeSeriesReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DebuggingTest {

    @Test
    void test(){
        System.out.println((double)10F);
    }


    @Test
    void debugTest() throws IOException {
        List<TimeSeriesReading> timeSeriesReadings = extractAllReadings("./src/test/java/debugging/data.csv");
        List<DataPoint> dataPoints = getdataPointsFromReadings(timeSeriesReadings);

        int threshold = 1000;

        DeltaDeltaTimestampCompressionModel deltaDeltaTimestampCompressionModel = new DeltaDeltaTimestampCompressionModel(threshold, 200);
        boolean b = deltaDeltaTimestampCompressionModel.resetAndAppendAll(dataPoints);

        List<Long> longs = decompressTimestampsForTimestampModel(dataPoints, deltaDeltaTimestampCompressionModel);



        for (int i = 0; i < dataPoints.size(); i++) {
            Assertions.assertTrue(Math.abs(dataPoints.get(i).timestamp() - longs.get(i)) < threshold);
        }
    }

    private List<Long> decompressTimestampsForTimestampModel(List<DataPoint> dataPoints, TimestampCompressionModel timestampCompressionModel) {
        return BlobDecompressor.decompressTimestampsUsingAmtDataPoints(
                timestampCompressionModel.getTimestampCompressionModelType(),
                timestampCompressionModel.getBlobRepresentation(),
                dataPoints.get(0).timestamp(),
                timestampCompressionModel.getLength()
        );
    }


    private List<DataPoint> getdataPointsFromReadings(List<TimeSeriesReading> readings) {
        return readings.stream().map(TimeSeriesReading::getDataPoint).toList();
    }

    private List<TimeSeriesReading> extractAllReadings(String pathname) throws IOException {
        File testData = new File(pathname);
        CSVTimeSeriesReader csvTimeSeriesReader = new CSVTimeSeriesReader(testData, " ");

        ArrayList<TimeSeriesReading> result = new ArrayList<>();
        TimeSeriesReading reading = csvTimeSeriesReader.next();
        while (Objects.nonNull(reading)) {
            result.add(reading);
            reading = csvTimeSeriesReader.next();
        }
        return result;
    }

}
