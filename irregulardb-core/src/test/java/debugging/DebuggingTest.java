package debugging;


import compression.BlobDecompressor;
import compression.CompressionModelFactory;
import compression.timestamp.DeltaDeltaTimestampCompressionModel;
import compression.timestamp.RegularTimestampCompressionModel;
import compression.timestamp.SIDiffTimestampCompressionModel;
import compression.timestamp.TimestampCompressionModel;
import config.ConfigProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import records.DataPoint;
import records.Segment;
import records.TimeSeriesReading;
import segmentgenerator.CompressionModelManager;
import segmentgenerator.ModelPicker;
import segmentgenerator.ModelPickerFactory;
import segmentgenerator.SegmentGenerator;
import sources.CSVTimeSeriesReader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class DebuggingTest {

    @BeforeAll
    public static void beforeAll() {
        ConfigProperties.isTest = true;
        ConfigProperties.getInstance().setProperty("model.length_bound", "200");
        ConfigProperties.getInstance().setProperty("model.timestamp.threshold", "1000");
    }


    @Test

    void debugTest() throws IOException {
        List<TimeSeriesReading> timeSeriesReadings = extractAllReadings("./src/test/java/debugging/data1.csv");
        List<DataPoint> dataPoints = getdataPointsFromReadings(timeSeriesReadings);

        int threshold = 1000;

        TimestampCompressionModel timestampCompressionModel = new RegularTimestampCompressionModel(threshold);
        boolean b = timestampCompressionModel.resetAndAppendAll(dataPoints);

        List<Long> decompressedTimestamps = decompressTimestampsForTimestampModel(dataPoints, timestampCompressionModel);

        long previousTime = Long.MIN_VALUE;

        for (int i = 0; i < decompressedTimestamps.size(); i++) {

            Assertions.assertTrue(previousTime < decompressedTimestamps.get(i));

            long diff = Math.abs(dataPoints.get(i).timestamp() - decompressedTimestamps.get(i));
            Assertions.assertTrue(diff <= threshold);
            previousTime = decompressedTimestamps.get(i);
        }
    }

    @Test
    void debugTestSIDiff() throws IOException {
        List<TimeSeriesReading> timeSeriesReadings = extractAllReadings("./src/test/java/debugging/data.csv");
        List<DataPoint> dataPoints = getdataPointsFromReadings(timeSeriesReadings);

        int threshold = 1000;

        SIDiffTimestampCompressionModel siDiffTimestampCompressionModel = new SIDiffTimestampCompressionModel(threshold, 200);
        boolean b = siDiffTimestampCompressionModel.resetAndAppendAll(dataPoints);

        List<Long> decompressedTimestamps = decompressTimestampsForTimestampModel(dataPoints, siDiffTimestampCompressionModel);

        long previousTime = Long.MIN_VALUE;

        for (int i = 0; i < decompressedTimestamps.size(); i++) {

            Assertions.assertTrue(previousTime < decompressedTimestamps.get(i));

            long diff = Math.abs(dataPoints.get(i).timestamp() - decompressedTimestamps.get(i));
            Assertions.assertTrue(diff <= threshold);
            previousTime = decompressedTimestamps.get(i);
        }
    }

    @Test
    void debugTestSIDiffMultipleSegments() throws IOException {
        List<TimeSeriesReading> timeSeriesReadings = extractAllReadings("./src/test/java/debugging/data.csv");
        List<DataPoint> dataPoints = getdataPointsFromReadings(timeSeriesReadings);
        int threshold = 1000;
        SIDiffTimestampCompressionModel siDiffTimestampCompressionModel = new SIDiffTimestampCompressionModel(threshold, 200);

        boolean b = siDiffTimestampCompressionModel.resetAndAppendAll(dataPoints);

        List<Long> decompressedTimestamps = decompressTimestampsForTimestampModel(dataPoints, siDiffTimestampCompressionModel);

        long previousTime = Long.MIN_VALUE;

        for (int i = 0; i < decompressedTimestamps.size(); i++) {

            Assertions.assertTrue(previousTime < decompressedTimestamps.get(i));

            long diff = Math.abs(dataPoints.get(i).timestamp() - decompressedTimestamps.get(i));
            Assertions.assertTrue(diff <= threshold);
            previousTime = decompressedTimestamps.get(i);
        }

        List<TimeSeriesReading> timeSeriesReadingsSegment2 = extractAllReadings("./src/test/java/debugging/data2.csv");
        List<DataPoint> dataPointsSegment2 = getdataPointsFromReadings(timeSeriesReadings);
        SIDiffTimestampCompressionModel siDiffTimestampCompressionModelSegment2 = new SIDiffTimestampCompressionModel(threshold, 200);

        boolean b1 = siDiffTimestampCompressionModelSegment2.resetAndAppendAll(dataPointsSegment2);

        List<Long> decompressedTimestampsSegment2 = decompressTimestampsForTimestampModel(dataPoints, siDiffTimestampCompressionModelSegment2);

        previousTime = Long.MIN_VALUE;

        for (int i = 0; i < decompressedTimestampsSegment2.size(); i++) {

            Assertions.assertTrue(previousTime < decompressedTimestampsSegment2.get(i));

            long diff = Math.abs(dataPointsSegment2.get(i).timestamp() - decompressedTimestampsSegment2.get(i));
            Assertions.assertTrue(diff <= threshold);
            previousTime = decompressedTimestamps.get(i);
        }

        Assertions.assertNotSame(decompressedTimestamps.get(decompressedTimestamps.size() - 1), decompressedTimestampsSegment2.get(0));
    }

    @Test
    void SegmentGeneratorDebugTest() throws IOException {
        List<TimeSeriesReading> timeSeriesReadings = extractAllReadings("./src/test/java/debugging/data1.csv");
        List<DataPoint> dataPoints = getdataPointsFromReadings(timeSeriesReadings);

        int threshold = 1000;
        ModelPicker modelPicker = ModelPickerFactory.createModelPickerFromConfig();
        CompressionModelManager compressionModelManager = new CompressionModelManager(
                CompressionModelFactory.getValueCompressionModels(""),
                List.of(new RegularTimestampCompressionModel(threshold)),
                modelPicker);
        SegmentGenerator segmentGenerator = new SegmentGenerator(compressionModelManager, 1);

        List<Segment> segments = new ArrayList<>();
        for (DataPoint dataPoint : dataPoints) {
            boolean accepted = segmentGenerator.acceptDataPoint(dataPoint);
            if (!accepted) {
                segments.addAll(segmentGenerator.constructSegmentsFromBuffer());
            }
        }

        int a = 0;
    }

    @Test
    void esbenDebug() {
        int threshold = 100;
        //List<Long> timeStamps = List.of(0L, 2000L, 3000L, 4999L, 5000L);
        List<Long> timeStamps = List.of(0L, 200L, 300L, 350L);
        //List<Long> timeStamps = List.of(23000L, 24000L, 26000L, 28001L, 31000L); // NOT a problem apperently
        //List<Long> timeStamps = List.of(23000L, 25000L, 26000L, 28000L, 29000L);

        List<DataPoint> dataPoints = createDataPointsFromTimestamps(timeStamps);

        TimestampCompressionModel deltaDelta = new DeltaDeltaTimestampCompressionModel(threshold, 200);
        boolean b = deltaDelta.resetAndAppendAll(dataPoints);

        List<Long> decompressedTimestamps = decompressTimestampsForTimestampModel(dataPoints, deltaDelta);
        for (int i = 0; i < dataPoints.size(); i++) {
            Assertions.assertTrue(Math.abs(dataPoints.get(i).timestamp() - decompressedTimestamps.get(i)) < threshold);
        }
    }

    private List<DataPoint> createDataPointsFromTimestamps(List<Long> timestamps) {
        return timestamps.stream().map(t -> new DataPoint(t, -1)).toList();
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
        CSVTimeSeriesReader csvTimeSeriesReader = new CSVTimeSeriesReader(testData,"", " ");

        ArrayList<TimeSeriesReading> result = new ArrayList<>();
        TimeSeriesReading reading = csvTimeSeriesReader.next();
        while (Objects.nonNull(reading)) {
            result.add(reading);
            reading = csvTimeSeriesReader.next();
        }
        return result;
    }

}
