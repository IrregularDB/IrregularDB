package compression;

import compression.timestamp.*;
import compression.value.GorillaValueCompressionModel;
import compression.value.ValueCompressionModel;
import compression.value.ValueCompressionModelType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class BlobDecompressorTest {
    TimeStampCompressionModelType timeStampModelType;
    ValueCompressionModelType valueModelType;
    ByteBuffer timeStampBlob;
    ByteBuffer valueBlob;

    @BeforeEach
    void beforeEach(){
        // Done in order to ensure none of the tests affect each other
        timeStampModelType = null;
        valueModelType = null;
        timeStampBlob = null;
        valueBlob = null;
    }

    List<Long> callTimeStampDecompressor(long startTime, long endTime) {
        if (timeStampModelType == null || timeStampBlob == null) {
            throw new RuntimeException("You forgot to setup time stamp compression model. Call for example setupRegularTimeStamp() before calling this method");
        }

        return BlobDecompressor.decompressTimeStamps(timeStampModelType, timeStampBlob, startTime, endTime);
    }

    List<DataPoint> callValueDecompressor(List<Long> timeStamps) {
        if (valueModelType == null || valueBlob == null) {
            throw new RuntimeException("You forgot to setup value compression model. Call for example setupPMCMeanValue() before calling this method");
        }
        return BlobDecompressor.createDataPointsByDecompressingValues(valueModelType, valueBlob, timeStamps);
    }

    void setupRegularTimeStampModel(int si) {
        timeStampModelType = TimeStampCompressionModelType.REGULAR;
        timeStampBlob = ByteBuffer.allocate(4).putInt(si);
    }

    void setupPMCMeanValueModel(float mean) {
        valueModelType = ValueCompressionModelType.PMC_MEAN;
        valueBlob = ByteBuffer.allocate(4).putFloat(mean);
    }

    void setupSwingValueModel(float slope, float intercept) {
        valueModelType = ValueCompressionModelType.SWING;
        valueBlob = ByteBuffer.allocate(8).putFloat(slope).putFloat(intercept);
    }

    void setupGorillaValueModel(List<DataPoint> dataPoints) {
        valueModelType = ValueCompressionModelType.GORILLA;
        ValueCompressionModel gorillaModel = new GorillaValueCompressionModel(dataPoints.size());
        gorillaModel.resetAndAppendAll(dataPoints);
        valueBlob = gorillaModel.getBlobRepresentation();
    }

    /***
     * REGULAR MODEL TESTS:
     */
    @Test
    void decompressRegularSI100() {
        setupRegularTimeStampModel(100);

        long startTime = 0;
        long endTime = 500;

        List<Long> timeStamps = callTimeStampDecompressor(startTime, endTime);

        List<Long> expectedTimeStamps =  List.of(0L, 100L, 200L, 300L, 400L, 500L);
        assertEquals(expectedTimeStamps, timeStamps);
    }

    @Test
    void decompressRegularWeirdSI() {
        setupRegularTimeStampModel(55);
        long startTime = 110;
        long endTime = 275;
        List<Long> timeStamps = callTimeStampDecompressor(startTime, endTime);

        List<Long> expectedTimeStamps =  List.of(110L, 165L, 220L, 275L);
        assertEquals(expectedTimeStamps, timeStamps);
    }

    @Test
    void decompressRegularEndTimeDoesNotAlign() {
        // We here expect it the last time stamp to be the last time the time stamp aligned with SI before the end time
        // i.e. we don't expect it to add 400
        setupRegularTimeStampModel(100);

        long startTime = 0;
        long endTime = 301;

        List<Long> timeStamps = callTimeStampDecompressor(startTime, endTime);

        List<Long> expectedTimeStamps =  List.of(0L, 100L, 200L, 300L);
        assertEquals(expectedTimeStamps, timeStamps);
    }

    /***
     * PMC-MEAN MODEL TESTS:
     */
    @Test
    void decompressPMCMean() {
        float meanValue = 2.5F;
        setupPMCMeanValueModel(meanValue);
        List<Long> timeStamps = List.of(0L, 100L, 200L, 300L, 400L, 500L);
        List<DataPoint> dataPoints = callValueDecompressor(timeStamps);
        List<DataPoint> expectedDataPoints = new ArrayList<>();
        expectedDataPoints.add(new DataPoint(0, meanValue));
        expectedDataPoints.add(new DataPoint(100, meanValue));
        expectedDataPoints.add(new DataPoint(200, meanValue));
        expectedDataPoints.add(new DataPoint(300, meanValue));
        expectedDataPoints.add(new DataPoint(400, meanValue));
        expectedDataPoints.add(new DataPoint(500, meanValue));
        assertEquals(expectedDataPoints, dataPoints);
    }

    @Test
    void decompressPMCMeanIrregular() {
        setupPMCMeanValueModel(5.0F);

        List<Long> timeStamps = List.of(0L, 75L, 200L, 300L, 500L);
        List<DataPoint> dataPoints = callValueDecompressor(timeStamps);
        List<DataPoint> expectedDataPoints = new ArrayList<>();
        expectedDataPoints.add(new DataPoint(0, 5.0F));
        expectedDataPoints.add(new DataPoint(75, 5.0F));
        expectedDataPoints.add(new DataPoint(200, 5.0F));
        expectedDataPoints.add(new DataPoint(300, 5.0F));
        expectedDataPoints.add(new DataPoint(500, 5.0F));
        assertEquals(expectedDataPoints, dataPoints);
    }

    /***
     * SWING MODEL TESTS:
     */
    @Test
    void decompressSwing() {
        setupSwingValueModel(0.05F, 0.00F);

        List<Long> timeStamps = List.of(0L, 1L, 2L, 3L, 4L, 5L);
        List<DataPoint> dataPoints = callValueDecompressor(timeStamps);
        List<DataPoint> expectedDataPoints = new ArrayList<>();
        expectedDataPoints.add(new DataPoint(0, 0));
        expectedDataPoints.add(new DataPoint(1, 0.05F));
        expectedDataPoints.add(new DataPoint(2, 0.1F));
        expectedDataPoints.add(new DataPoint(3, 0.15F));
        expectedDataPoints.add(new DataPoint(4, 0.20F));
        expectedDataPoints.add(new DataPoint(5, 0.25F));

        var allowedDifference = 0.000001;
        for (int i = 0; i < dataPoints.size(); i++) {
            DataPoint actualDataPoint = dataPoints.get(i);
            DataPoint expectedDataPoint = expectedDataPoints.get(i);
            double difference = actualDataPoint.value() - expectedDataPoint.value();
            assertTrue(difference < allowedDifference);
            assertEquals(expectedDataPoint.timestamp(), actualDataPoint.timestamp());
        }
    }

    @Test
    void decompressSwingNegativeSlope() {
        setupSwingValueModel(-0.05F, 10.00F);

        List<Long> timeStamps = List.of(0L, 1L, 2L, 3L, 4L, 5L);
        List<DataPoint> expectedDataPoints = new ArrayList<>();
        expectedDataPoints.add(new DataPoint(0, 10.00F));
        expectedDataPoints.add(new DataPoint(1, 9.95F));
        expectedDataPoints.add(new DataPoint(2, 9.90F));
        expectedDataPoints.add(new DataPoint(3, 9.85F));
        expectedDataPoints.add(new DataPoint(4, 9.80F));
        expectedDataPoints.add(new DataPoint(5, 9.75F));

        List<DataPoint> actualDataPoints = callValueDecompressor(timeStamps);
        var allowedDifference = 0.000001;
        for (int i = 0; i < actualDataPoints.size(); i++) {
            DataPoint actualDataPoint = actualDataPoints.get(i);
            DataPoint expectedDataPoint = expectedDataPoints.get(i);
            double difference = actualDataPoint.value() - expectedDataPoint.value();
            assertTrue(difference < allowedDifference);
            assertEquals(expectedDataPoint.timestamp(), actualDataPoint.timestamp());
        }
    }

    @Test
    void decompressGorilla() {
        List<DataPoint> expectedDataPoints = new ArrayList<>();
        expectedDataPoints.add(new DataPoint(0, 10.00F));
        expectedDataPoints.add(new DataPoint(1, 33.0F));
        expectedDataPoints.add(new DataPoint(2, 45.0F));
        expectedDataPoints.add(new DataPoint(3, 45.0F));
        expectedDataPoints.add(new DataPoint(4, 90.0F));
        expectedDataPoints.add(new DataPoint(5, 10.0F));

        List<Long> timeStamps = expectedDataPoints.stream()
                .map(DataPoint::timestamp).toList();

        setupGorillaValueModel(expectedDataPoints);
        List<DataPoint> actualDataPoints = callValueDecompressor(timeStamps);

        assertEquals(expectedDataPoints, actualDataPoints);
    }

    @Test
    void decompressGorillaIrregularTime() {
        List<DataPoint> expectedDataPoints = new ArrayList<>();
        expectedDataPoints.add(new DataPoint(0, 10.00F));
        expectedDataPoints.add(new DataPoint(3, 33.0F));
        expectedDataPoints.add(new DataPoint(7, 45.0F));
        expectedDataPoints.add(new DataPoint(10, 45.0F));
        expectedDataPoints.add(new DataPoint(14, 90.0F));
        expectedDataPoints.add(new DataPoint(15, 10.0F));

        List<Long> timeStamps = expectedDataPoints.stream()
                .map(DataPoint::timestamp).toList();

        setupGorillaValueModel(expectedDataPoints);
        List<DataPoint> actualDataPoints = callValueDecompressor(timeStamps);

        assertEquals(expectedDataPoints, actualDataPoints);
    }

    /***
     * We test a combination of two methods
     */
    @Test
    void decompressRegularAndPMCMean() {
        setupRegularTimeStampModel(100);
        setupPMCMeanValueModel(5.0F);

        long startTime = 0;
        long endTime = 500;
        List<DataPoint> dataPoints = BlobDecompressor.decompressBlobs(timeStampModelType, timeStampBlob, valueModelType, valueBlob, startTime, endTime);

        List<DataPoint> expectedDataPoints = new ArrayList<>();
        expectedDataPoints.add(new DataPoint(0, 5.0F));
        expectedDataPoints.add(new DataPoint(100, 5.0F));
        expectedDataPoints.add(new DataPoint(200, 5.0F));
        expectedDataPoints.add(new DataPoint(300, 5.0F));
        expectedDataPoints.add(new DataPoint(400, 5.0F));
        expectedDataPoints.add(new DataPoint(500, 5.0F));

        assertEquals(expectedDataPoints, dataPoints);
    }

    /**
     * BaseDelta decompression tests
     */
    @Test
    public void testBaseDeltaDecompression(){
        List<DataPoint> expectedDataPoints = new ArrayList<>();
        long startTime = 0;
        long endTime = 500;
        expectedDataPoints.add(new DataPoint(startTime, 5.0F));
        expectedDataPoints.add(new DataPoint(100, 5.0F));
        expectedDataPoints.add(new DataPoint(200, 5.0F));
        expectedDataPoints.add(new DataPoint(300, 5.0F));
        expectedDataPoints.add(new DataPoint(400, 5.0F));
        expectedDataPoints.add(new DataPoint(endTime, 5.0F));

        List<Long> expectedTimeStamps = expectedDataPoints.stream()
                .map(DataPoint::timestamp)
                .collect(Collectors.toList());

        var timeStampCompressionModel = new BaseDeltaTimeStampCompressionModel();
        boolean appendSucceeded = timeStampCompressionModel.resetAndAppendAll(expectedDataPoints);
        ByteBuffer blobRepresentation = timeStampCompressionModel.getBlobRepresentation();

        List<Long> actualTimeStampsDecompressed = BlobDecompressor
                .decompressTimeStamps(TimeStampCompressionModelType.BASEDELTA,
                        blobRepresentation,
                        startTime,
                        endTime
                );

        Assertions.assertTrue(appendSucceeded);
        Assertions.assertEquals(expectedTimeStamps, actualTimeStampsDecompressed);
    }

    /**
     * Delta timestamp compression
     */
    @Test
    public void testDeltaTimestampCompression(){
        List<DataPoint> expectedDataPoints = new ArrayList<>();
        expectedDataPoints.add(new DataPoint(0, 5.0F));
        expectedDataPoints.add(new DataPoint(100, 5.0F));
        expectedDataPoints.add(new DataPoint(2000, 5.0F));
        expectedDataPoints.add(new DataPoint(4000, 5.0F));
        expectedDataPoints.add(new DataPoint(6000, 5.0F));
        expectedDataPoints.add(new DataPoint(27000, 5.0F));
        expectedDataPoints.add(new DataPoint(Integer.MAX_VALUE, 5.0F));

        List<Long> expectedTimestamps = expectedDataPoints.stream().map(DataPoint::timestamp).toList();

        DeltaPairsTimeStampCompressionModel deltaPairsTimeStampCompressionModel = new DeltaPairsTimeStampCompressionModel();
        deltaPairsTimeStampCompressionModel.resetAndAppendAll(expectedDataPoints);

        ByteBuffer blobRepresentation = deltaPairsTimeStampCompressionModel.getBlobRepresentation();

        List<Long> decompressedTimeStamps = BlobDecompressor.decompressTimeStamps(TimeStampCompressionModelType.DELTAPAIRS, blobRepresentation, expectedDataPoints.get(0).timestamp(), expectedDataPoints.get(expectedDataPoints.size() - 1).timestamp());

        Assertions.assertEquals(expectedTimestamps.size(), decompressedTimeStamps.size());

        for (int i = 0; i < expectedTimestamps.size(); i++) {
            Assertions.assertEquals(expectedTimestamps.get(i), decompressedTimeStamps.get(i));
        }
    }

    /**
     * DeltaDelta timestamp compression
     */

    @Test
    public void testDeltaDeltaTimeStampCompression(){
        TimeStampCompressionModel deltaDeltaTimeStampCompression = new DeltaDeltaTimeStampCompression();

        List<DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(new DataPoint(0, 5.0F));
        dataPoints.add(new DataPoint(100, 5.0F));
        dataPoints.add(new DataPoint(2000, 5.0F));
        dataPoints.add(new DataPoint(4000, 5.0F));
        dataPoints.add(new DataPoint(6000, 5.0F));
        dataPoints.add(new DataPoint(27000, 5.0F));
        dataPoints.add(new DataPoint(Integer.MAX_VALUE, 5.0F));

        dataPoints.forEach(dp -> deltaDeltaTimeStampCompression.append(dp));

        var blobRepresentation = deltaDeltaTimeStampCompression.getBlobRepresentation();

        var decodedTimestamps = BlobDecompressor.decompressTimeStamps(TimeStampCompressionModelType.DELTADELTA,
                blobRepresentation, dataPoints.get(0).timestamp(), dataPoints.get(dataPoints.size() - 1).timestamp());

        for (int i = 0; i < decodedTimestamps.size(); i++){
            Assertions.assertEquals(dataPoints.get(i).timestamp(), decodedTimestamps.get(i));
        }
    }

}