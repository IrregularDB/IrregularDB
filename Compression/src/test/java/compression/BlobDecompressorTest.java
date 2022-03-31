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
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class BlobDecompressorTest {
    TimeStampCompressionModelType timeStampModelType;
    ValueCompressionModelType valueModelType;
    ByteBuffer timeStampBlob;
    ByteBuffer valueBlob;

    // Helper that creates random data points in increasing order
    private DataPoint createRandomDataPoint(Random random){
        long previousLong = 0;
        float previousFloat = 0;

        previousLong += random.nextLong(100L);
        previousFloat += random.nextFloat(100.00f);
        return new DataPoint(previousLong, previousFloat);
    }

    private List<DataPoint> createXRandomDataPoints(int x){
        Random random = new Random();

        List<DataPoint> dataPointList = new ArrayList<>();
        for(int i = 0; i < x; i++){
            dataPointList.add(createRandomDataPoint(random));
        }
        return dataPointList;
    }

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

    @Test
    public void testDeltaDeltaRandomDataPoints(){
        TimeStampCompressionModel deltaDeltaTimeStampCompression = new DeltaDeltaTimeStampCompression();

        List<DataPoint> dataPoints = createXRandomDataPoints(10);
        deltaDeltaTimeStampCompression.resetAndAppendAll(dataPoints);

        var blobRepresentation = deltaDeltaTimeStampCompression.getBlobRepresentation();

        var decodedTimestamps = BlobDecompressor.decompressTimeStamps(TimeStampCompressionModelType.DELTADELTA,
                blobRepresentation, dataPoints.get(0).timestamp(), dataPoints.get(dataPoints.size() - 1).timestamp());

        for (int i = 0; i < decodedTimestamps.size(); i++){
            Assertions.assertEquals(dataPoints.get(i).timestamp(), decodedTimestamps.get(i));
        }
    }


    /**
     * SI-diff timestamp compression
     */
    @Test
    public void testSIDiffRandomDataPointsNoErrorBound(){
        float errorBound = 0.0F;
        TimeStampCompressionModel SIDiffTimeStampCompression = new SIDiffTimeStampCompressionModel(errorBound);

        List<DataPoint> dataPoints = createXRandomDataPoints(10);
        SIDiffTimeStampCompression.resetAndAppendAll(dataPoints);

        var blobRepresentation = SIDiffTimeStampCompression.getBlobRepresentation();

        var decodedTimestamps = BlobDecompressor.decompressTimeStamps(TimeStampCompressionModelType.SIDIFF,
                blobRepresentation, dataPoints.get(0).timestamp(), dataPoints.get(dataPoints.size() - 1).timestamp());

        for (int i = 0; i < decodedTimestamps.size(); i++){
            Assertions.assertEquals(dataPoints.get(i).timestamp(), decodedTimestamps.get(i));
        }
    }

    @Test
    public void testSIDiffNoErrorBound(){
        float errorBound = 0.0F;
        TimeStampCompressionModel SIDiffTimeStampCompression = new SIDiffTimeStampCompressionModel(errorBound);

        List<DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(new DataPoint(0, 5.0F));
        dataPoints.add(new DataPoint(100, 5.0F));
        dataPoints.add(new DataPoint(205, 5.0F));
        dataPoints.add(new DataPoint(300, 5.0F));
        dataPoints.add(new DataPoint(395, 5.0F));
        dataPoints.add(new DataPoint(500, 5.0F));

        SIDiffTimeStampCompression.resetAndAppendAll(dataPoints);

        var blobRepresentation = SIDiffTimeStampCompression.getBlobRepresentation();

        var decodedTimestamps = BlobDecompressor.decompressTimeStamps(TimeStampCompressionModelType.SIDIFF,
                blobRepresentation, dataPoints.get(0).timestamp(), dataPoints.get(dataPoints.size() - 1).timestamp());

        for (int i = 0; i < decodedTimestamps.size(); i++){
            Assertions.assertEquals(dataPoints.get(i).timestamp(), decodedTimestamps.get(i));
        }
    }

    private boolean isTimestampWithinErrorBound(long actualTimestamp, long recreatedTimestamp, int si, float errorBound) {
        long difference = Math.abs(actualTimestamp - recreatedTimestamp);
        if (difference < Integer.MIN_VALUE || difference > Integer.MAX_VALUE) {
            throw new SiConversionException(difference  + " the difference in timestamps cannot be cast to int without changing its value.");
        }
        double percentageError = difference / ((double)si);
        return percentageError <= errorBound;
    }

    private int calculateSI(List<DataPoint> dataPoints) {
        return Math.round((float) (dataPoints.get(dataPoints.size() - 1).timestamp() - dataPoints.get(0).timestamp()) / (dataPoints.size() - 1));
    }

    @Test
    public void testSIDiffRandomDataPoints10PercentError(){
        float errorBound = 10.0F;
        TimeStampCompressionModel SIDiffTimeStampCompression = new SIDiffTimeStampCompressionModel(errorBound);

        List<DataPoint> dataPoints = createXRandomDataPoints(10);
        SIDiffTimeStampCompression.resetAndAppendAll(dataPoints);

        var blobRepresentation = SIDiffTimeStampCompression.getBlobRepresentation();

        var decodedTimestamps = BlobDecompressor.decompressTimeStamps(TimeStampCompressionModelType.SIDIFF,
                blobRepresentation, dataPoints.get(0).timestamp(), dataPoints.get(dataPoints.size() - 1).timestamp());

        int si = calculateSI(dataPoints);

        for (int i = 0; i < decodedTimestamps.size(); i++){
            Assertions.assertTrue(isTimestampWithinErrorBound(dataPoints.get(i).timestamp(), decodedTimestamps.get(i), si, errorBound));
        }
    }

    @Test
    public void testSIDiff10PercentError(){
        float errorBound = 10.0F;
        TimeStampCompressionModel SIDiffTimeStampCompression = new SIDiffTimeStampCompressionModel(errorBound);

        List<DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(new DataPoint(0, 5.0F));
        dataPoints.add(new DataPoint(100, 5.0F));
        dataPoints.add(new DataPoint(205, 5.0F));
        dataPoints.add(new DataPoint(300, 5.0F));
        dataPoints.add(new DataPoint(395, 5.0F));
        dataPoints.add(new DataPoint(500, 5.0F));

        SIDiffTimeStampCompression.resetAndAppendAll(dataPoints);

        var blobRepresentation = SIDiffTimeStampCompression.getBlobRepresentation();

        var decodedTimestamps = BlobDecompressor.decompressTimeStamps(TimeStampCompressionModelType.SIDIFF,
                blobRepresentation, dataPoints.get(0).timestamp(), dataPoints.get(dataPoints.size() - 1).timestamp());

        int si = calculateSI(dataPoints);

        for (int i = 0; i < decodedTimestamps.size(); i++){
            Assertions.assertTrue(isTimestampWithinErrorBound(dataPoints.get(i).timestamp(), decodedTimestamps.get(i), si, errorBound));
        }
    }

    @Test
    public void testSIDiffThatTheyGetMovedBucketsDown(){
        float errorBound = 10.0F;
        TimeStampCompressionModel SIDiffTimeStampCompression = new SIDiffTimeStampCompressionModel(errorBound);

        // We have 5 data points and last time stamp is 10000 so we get:
        // SI = 2500
        // Allowed derivation = 2500 * 10 % = 250 buckets are in theory 250 larger
        List<DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(new DataPoint(0, 5.0F));
        dataPoints.add(new DataPoint(2500 + 250, 5.0F));
        dataPoints.add(new DataPoint(5000 + 251, 5.0F));
        dataPoints.add(new DataPoint(7500 + (511 + 250), 5.0F));
        dataPoints.add(new DataPoint(10000, 5.0F));

        SIDiffTimeStampCompression.resetAndAppendAll(dataPoints);

        var blobRepresentation = SIDiffTimeStampCompression.getBlobRepresentation();

        var decodedTimestamps = BlobDecompressor.decompressTimeStamps(TimeStampCompressionModelType.SIDIFF,
                blobRepresentation, dataPoints.get(0).timestamp(), dataPoints.get(dataPoints.size() - 1).timestamp());

        assertEquals(0, decodedTimestamps.get(0));
        assertEquals(2500, decodedTimestamps.get(1)); // 2500 + 250 -> 2500, should get reduced to bucket 0.
        assertEquals(5000 + 251, decodedTimestamps.get(2)); // 5000 + 251 -> 5251, because it is above the threshold for bucket 0 and should stay in bucket 1.
        assertEquals(7500 + 511, decodedTimestamps.get(3)); // 7500 + (511 + 250) -> 7500 + 511, bucket 1 size is 511 so we want to be 511+250 away to get reduced to it its max value
        assertEquals(10000, decodedTimestamps.get(4)); // 10000 -> 10000 because it is precisely reconstructed using the calculated SI.
    }

}