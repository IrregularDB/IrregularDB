package compression;

import compression.timestamp.TimeStampCompressionModelType;
import compression.value.ValueCompressionModelType;
import org.junit.jupiter.api.Assertions;
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

    void setupRegularTimeStamp(int si) {
        timeStampModelType = TimeStampCompressionModelType.REGULAR;
        timeStampBlob = ByteBuffer.allocate(4).putInt(si);
    }

    void setupPMCMeanValue(float mean) {
        valueModelType = ValueCompressionModelType.PMCMEAN;
        valueBlob = ByteBuffer.allocate(4).putFloat(mean);
    }

    @Test
    void decompressRegularSI100() {
        setupRegularTimeStamp(100);

        long startTime = 0;
        long endTime = 500;

        List<Long> timeStamps = callTimeStampDecompressor(startTime, endTime);

        List<Long> expectedTimeStamps =  List.of(0L, 100L, 200L, 300L, 400L, 500L);
        assertEquals(expectedTimeStamps, timeStamps);
    }

    @Test
    void decompressRegularWeirdSI() {
        setupRegularTimeStamp(55);
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
        setupRegularTimeStamp(100);

        long startTime = 0;
        long endTime = 301;

        List<Long> timeStamps = callTimeStampDecompressor(startTime, endTime);

        List<Long> expectedTimeStamps =  List.of(0L, 100L, 200L, 300L);
        assertEquals(expectedTimeStamps, timeStamps);
    }

    @Test
    void decompressPMCMeanRegularTimeStamps() {
        float meanValue = 2.5F;
        setupPMCMeanValue(meanValue);
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
    void decompressPMCMeanIrregularTimeStamps() {
        setupPMCMeanValue(5.0F);

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

    /**
     * From here we add a few tests, where we combined the two methods to ensure they also work together
     */
    @Test
    void decompressRegularAndPMCMean() {
        setupRegularTimeStamp(100);
        setupPMCMeanValue(5.0F);

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