package compression;

import compression.timestamp.TimeStampCompressionModelType;
import compression.value.ValueCompressionModelType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

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
        return BlobDecompressor.decompressValuesAndCreateDataPoints(valueModelType, valueBlob, timeStamps);
    }

    void setupRegularTimeStampModel(int si) {
        timeStampModelType = TimeStampCompressionModelType.REGULAR;
        timeStampBlob = ByteBuffer.allocate(4).putInt(si);
    }

    void setupPMCMeanValueModel(float mean) {
        valueModelType = ValueCompressionModelType.PMCMEAN;
        valueBlob = ByteBuffer.allocate(4).putFloat(mean);
    }

    void setupSwingValueModel(float slope, float intercept) {
        valueModelType = ValueCompressionModelType.SWING;
        valueBlob = ByteBuffer.allocate(8).putFloat(slope).putFloat(intercept);
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
        expectedDataPoints.add(new DataPoint(0, 5.0));
        expectedDataPoints.add(new DataPoint(75, 5.0));
        expectedDataPoints.add(new DataPoint(200, 5.0));
        expectedDataPoints.add(new DataPoint(300, 5.0));
        expectedDataPoints.add(new DataPoint(500, 5.0));
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
        expectedDataPoints.add(new DataPoint(1, 0.05));
        expectedDataPoints.add(new DataPoint(2, 0.1));
        expectedDataPoints.add(new DataPoint(3, 0.15));
        expectedDataPoints.add(new DataPoint(4, 0.20));
        expectedDataPoints.add(new DataPoint(5, 0.25));

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
        List<DataPoint> dataPoints = callValueDecompressor(timeStamps);
        List<DataPoint> expectedDataPoints = new ArrayList<>();
        expectedDataPoints.add(new DataPoint(0, 10.00));
        expectedDataPoints.add(new DataPoint(1, 9.95));
        expectedDataPoints.add(new DataPoint(2, 9.90));
        expectedDataPoints.add(new DataPoint(3, 9.85));
        expectedDataPoints.add(new DataPoint(4, 9.80));
        expectedDataPoints.add(new DataPoint(5, 9.75));

        var allowedDifference = 0.000001;
        for (int i = 0; i < dataPoints.size(); i++) {
            DataPoint actualDataPoint = dataPoints.get(i);
            DataPoint expectedDataPoint = expectedDataPoints.get(i);
            double difference = actualDataPoint.value() - expectedDataPoint.value();
            assertTrue(difference < allowedDifference);
            assertEquals(expectedDataPoint.timestamp(), actualDataPoint.timestamp());
        }
    }

    /***
     * From here we add a few tests, where we combine the two methods to ensure they also work together
     */
    @Test
    void decompressRegularAndPMCMean() {
        setupRegularTimeStampModel(100);
        setupPMCMeanValueModel(5.0F);

        long startTime = 0;
        long endTime = 500;
        List<DataPoint> dataPoints = BlobDecompressor.decompressBlobs(timeStampModelType, timeStampBlob, valueModelType, valueBlob, startTime, endTime);

        List<DataPoint> expectedDataPoints = new ArrayList<>();
        expectedDataPoints.add(new DataPoint(0, 5.0));
        expectedDataPoints.add(new DataPoint(100, 5.0));
        expectedDataPoints.add(new DataPoint(200, 5.0));
        expectedDataPoints.add(new DataPoint(300, 5.0));
        expectedDataPoints.add(new DataPoint(400, 5.0));
        expectedDataPoints.add(new DataPoint(500, 5.0));

        assertEquals(expectedDataPoints, dataPoints);
    }

}