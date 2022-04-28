package compression;

import compression.timestamp.*;
import compression.value.FallbackValueCompressionModel;
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

import static org.junit.jupiter.api.Assertions.*;

class BlobDecompressorTest {
    TimestampCompressionModelType timestampModelType;
    ValueCompressionModelType valueModelType;
    ByteBuffer timestampBlob;
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
        timestampModelType = null;
        valueModelType = null;
        timestampBlob = null;
        valueBlob = null;
    }

    List<Long> callTimestampDecompressor(long startTime, long endTime) {
        if (timestampModelType == null || timestampBlob == null) {
            throw new RuntimeException("You forgot to setup time stamp compression model. Call for example setupRegularTimeStamp() before calling this method");
        }

        return BlobDecompressor.decompressTimestamps(timestampModelType, timestampBlob, startTime, endTime);
    }

    List<DataPoint> callValueDecompressor(List<Long> timestamps) {
        if (valueModelType == null || valueBlob == null) {
            throw new RuntimeException("You forgot to setup value compression model. Call for example setupPMCMeanValue() before calling this method");
        }
        return BlobDecompressor.createDataPointsByDecompressingValues(valueModelType, valueBlob, timestamps);
    }

    void setupRegularTimestampModel(int si) {
        timestampModelType = TimestampCompressionModelType.REGULAR;
        timestampBlob = ByteBuffer.allocate(4).putInt(si);
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
        ValueCompressionModel gorillaModel = new GorillaValueCompressionModel(Integer.MAX_VALUE);
        gorillaModel.resetAndAppendAll(dataPoints);
        valueBlob = gorillaModel.getBlobRepresentation();
    }

    /***
     * REGULAR MODEL TESTS:
     */
    @Test
    void decompressRegularSI100() {
        setupRegularTimestampModel(100);

        long startTime = 0;
        long endTime = 500;

        List<Long> timestamps = callTimestampDecompressor(startTime, endTime);

        List<Long> expectedTimestamps =  List.of(0L, 100L, 200L, 300L, 400L, 500L);
        assertEquals(expectedTimestamps, timestamps);
    }

    @Test
    void decompressRegularWeirdSI() {
        setupRegularTimestampModel(55);
        long startTime = 110;
        long endTime = 275;
        List<Long> timestamps = callTimestampDecompressor(startTime, endTime);

        List<Long> expectedTimestamps =  List.of(110L, 165L, 220L, 275L);
        assertEquals(expectedTimestamps, timestamps);
    }

    @Test
    void decompressRegularEndTimeDoesNotAlign() {
        // We here expect it the last time stamp to be the last time the time stamp aligned with SI before the end time
        // i.e. we don't expect it to add 400
        setupRegularTimestampModel(100);

        long startTime = 0;
        long endTime = 301;

        List<Long> timestamps = callTimestampDecompressor(startTime, endTime);

        List<Long> expectedTimestamps =  List.of(0L, 100L, 200L, 300L);
        assertEquals(expectedTimestamps, timestamps);
    }

    /***
     * PMC-MEAN MODEL TESTS:
     */
    @Test
    void decompressPMCMean() {
        float meanValue = 2.5F;
        setupPMCMeanValueModel(meanValue);
        List<Long> timestamps = List.of(0L, 100L, 200L, 300L, 400L, 500L);
        List<DataPoint> dataPoints = callValueDecompressor(timestamps);
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

        List<Long> timestamps = List.of(0L, 75L, 200L, 300L, 500L);
        List<DataPoint> dataPoints = callValueDecompressor(timestamps);
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

        List<Long> timestamps = List.of(1000000000L, 1000000001L, 1000000002L, 1000000003L, 1000000004L, 1000000005L);
        List<DataPoint> dataPoints = callValueDecompressor(timestamps);
        List<DataPoint> expectedDataPoints = new ArrayList<>();
        expectedDataPoints.add(new DataPoint(1000000000, 1000000000 * 0.05F));
        expectedDataPoints.add(new DataPoint(1000000001, 1000000001 * 0.05F));
        expectedDataPoints.add(new DataPoint(1000000002, 1000000002 * 0.05F));
        expectedDataPoints.add(new DataPoint(1000000003, 1000000003 * 0.05F));
        expectedDataPoints.add(new DataPoint(1000000004, 1000000004 * 0.05F));
        expectedDataPoints.add(new DataPoint(1000000005, 1000000005 * 0.05F));

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

        List<Long> timestamps = List.of(0L, 1L, 2L, 3L, 4L, 5L);
        List<DataPoint> expectedDataPoints = new ArrayList<>();
        expectedDataPoints.add(new DataPoint(0, 10.00F));
        expectedDataPoints.add(new DataPoint(1, 9.95F));
        expectedDataPoints.add(new DataPoint(2, 9.90F));
        expectedDataPoints.add(new DataPoint(3, 9.85F));
        expectedDataPoints.add(new DataPoint(4, 9.80F));
        expectedDataPoints.add(new DataPoint(5, 9.75F));

        List<DataPoint> actualDataPoints = callValueDecompressor(timestamps);
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

        List<Long> timestamps = expectedDataPoints.stream()
                .map(DataPoint::timestamp).toList();

        setupGorillaValueModel(expectedDataPoints);
        List<DataPoint> actualDataPoints = callValueDecompressor(timestamps);

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

        List<Long> timestamps = expectedDataPoints.stream()
                .map(DataPoint::timestamp).toList();

        setupGorillaValueModel(expectedDataPoints);
        List<DataPoint> actualDataPoints = callValueDecompressor(timestamps);

        assertEquals(expectedDataPoints, actualDataPoints);
    }

    /***
     * We test a combination of two methods
     */
    @Test
    void decompressRegularAndPMCMean() {
        setupRegularTimestampModel(100);
        setupPMCMeanValueModel(5.0F);

        long startTime = 0;
        long endTime = 500;
        List<DataPoint> dataPoints = BlobDecompressor.decompressBlobs(timestampModelType, timestampBlob, valueModelType, valueBlob, startTime, endTime);

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
    public void testDeltaDeltaTimestampCompression(){
        TimestampCompressionModel deltaDeltaTimestampCompression = new DeltaDeltaTimestampCompressionModel(0, Integer.MAX_VALUE);

        List<DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(new DataPoint(0, 5.0F));
        dataPoints.add(new DataPoint(100, 5.0F));
        dataPoints.add(new DataPoint(2000, 5.0F));
        dataPoints.add(new DataPoint(4000, 5.0F));
        dataPoints.add(new DataPoint(6000, 5.0F));
        dataPoints.add(new DataPoint(27000, 5.0F));
        dataPoints.add(new DataPoint(Integer.MAX_VALUE, 5.0F));

        dataPoints.forEach(deltaDeltaTimestampCompression::append);

        var blobRepresentation = deltaDeltaTimestampCompression.getBlobRepresentation();

        var decodedTimestamps = BlobDecompressor.decompressTimestamps(TimestampCompressionModelType.DELTADELTA,
                blobRepresentation, dataPoints.get(0).timestamp(), dataPoints.get(dataPoints.size() - 1).timestamp());

        for (int i = 0; i < decodedTimestamps.size(); i++){
            Assertions.assertEquals(dataPoints.get(i).timestamp(), decodedTimestamps.get(i));
        }
    }

    @Test
    public void testDeltaDeltaRandomDataPoints(){
        TimestampCompressionModel deltaDeltaTimestampCompression = new DeltaDeltaTimestampCompressionModel(0, Integer.MAX_VALUE);

        List<DataPoint> dataPoints = createXRandomDataPoints(10);
        deltaDeltaTimestampCompression.resetAndAppendAll(dataPoints);

        var blobRepresentation = deltaDeltaTimestampCompression.getBlobRepresentation();

        var decodedTimestamps = BlobDecompressor.decompressTimestamps(TimestampCompressionModelType.DELTADELTA,
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
        TimestampCompressionModel SIDiffTimestampCompression = new SIDiffTimestampCompressionModel(0, Integer.MAX_VALUE);

        List<DataPoint> dataPoints = createXRandomDataPoints(10);
        SIDiffTimestampCompression.resetAndAppendAll(dataPoints);

        var blobRepresentation = SIDiffTimestampCompression.getBlobRepresentation();

        var decodedTimestamps = BlobDecompressor.decompressTimestamps(TimestampCompressionModelType.SIDIFF,
                blobRepresentation, dataPoints.get(0).timestamp(), dataPoints.get(dataPoints.size() - 1).timestamp());

        for (int i = 0; i < decodedTimestamps.size(); i++){
            Assertions.assertEquals(dataPoints.get(i).timestamp(), decodedTimestamps.get(i));
        }
    }

    @Test
    public void testSIDiffNoErrorBound(){
        TimestampCompressionModel SIDiffTimestampCompression = new SIDiffTimestampCompressionModel(0, Integer.MAX_VALUE);

        List<DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(new DataPoint(0, 5.0F));
        dataPoints.add(new DataPoint(100, 5.0F));
        dataPoints.add(new DataPoint(205, 5.0F));
        dataPoints.add(new DataPoint(300, 5.0F));
        dataPoints.add(new DataPoint(395, 5.0F));
        dataPoints.add(new DataPoint(500, 5.0F));

        SIDiffTimestampCompression.resetAndAppendAll(dataPoints);

        var blobRepresentation = SIDiffTimestampCompression.getBlobRepresentation();

        var decodedTimestamps = BlobDecompressor.decompressTimestamps(TimestampCompressionModelType.SIDIFF,
                blobRepresentation, dataPoints.get(0).timestamp(), dataPoints.get(dataPoints.size() - 1).timestamp());

        for (int i = 0; i < decodedTimestamps.size(); i++){
            Assertions.assertEquals(dataPoints.get(i).timestamp(), decodedTimestamps.get(i));
        }
    }

    private boolean isTimestampWithinThreshold(long actualTimestamp, long recreatedTimestamp, int threshold) {
        long difference = Math.abs(Math.toIntExact(actualTimestamp - recreatedTimestamp));
        return difference <= threshold;
    }

    @Test
    public void testSIDiffRandomDataPoints10PercentError(){
        int threshold = 10;
        TimestampCompressionModel SIDiffTimestampCompression = new SIDiffTimestampCompressionModel(threshold, Integer.MAX_VALUE);

        List<DataPoint> dataPoints = createXRandomDataPoints(10);
        SIDiffTimestampCompression.resetAndAppendAll(dataPoints);

        var blobRepresentation = SIDiffTimestampCompression.getBlobRepresentation();

        var decodedTimestamps = BlobDecompressor.decompressTimestamps(TimestampCompressionModelType.SIDIFF,
                blobRepresentation, dataPoints.get(0).timestamp(), dataPoints.get(dataPoints.size() - 1).timestamp());


        for (int i = 0; i < decodedTimestamps.size(); i++){
            Assertions.assertTrue(isTimestampWithinThreshold(dataPoints.get(i).timestamp(), decodedTimestamps.get(i), threshold));
        }
    }

    @Test
    public void testSIDiff10PercentError(){
        int threshold = 10;
        TimestampCompressionModel SIDiffTimestampCompression = new SIDiffTimestampCompressionModel(threshold, Integer.MAX_VALUE);

        List<DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(new DataPoint(0, 5.0F));
        dataPoints.add(new DataPoint(100, 5.0F));
        dataPoints.add(new DataPoint(205, 5.0F));
        dataPoints.add(new DataPoint(300, 5.0F));
        dataPoints.add(new DataPoint(395, 5.0F));
        dataPoints.add(new DataPoint(500, 5.0F));

        SIDiffTimestampCompression.resetAndAppendAll(dataPoints);

        var blobRepresentation = SIDiffTimestampCompression.getBlobRepresentation();

        var decodedTimestamps = BlobDecompressor.decompressTimestamps(TimestampCompressionModelType.SIDIFF,
                blobRepresentation, dataPoints.get(0).timestamp(), dataPoints.get(dataPoints.size() - 1).timestamp());

        for (int i = 0; i < decodedTimestamps.size(); i++){
            Assertions.assertTrue(isTimestampWithinThreshold(dataPoints.get(i).timestamp(), decodedTimestamps.get(i), threshold));
        }
    }

    @Test
    public void testSIDiffThatTheyGetMovedBucketsDown(){
        Integer threshold = 250;
        TimestampCompressionModel SIDiffTimestampCompression = new SIDiffTimestampCompressionModel(threshold, Integer.MAX_VALUE);

        // We have 5 data points and last time stamp is 10000 so we get:
        // SI = 2500
        // Allowed derivation = 2500 * 10 % = 250 buckets are in theory 250 larger
        List<DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(new DataPoint(0, 5.0F));
        dataPoints.add(new DataPoint(2500 + 250, 5.0F));
        dataPoints.add(new DataPoint(5000 + 251, 5.0F));
        dataPoints.add(new DataPoint(7500 + (511 + 250), 5.0F));
        dataPoints.add(new DataPoint(10000, 5.0F));

        SIDiffTimestampCompression.resetAndAppendAll(dataPoints);

        var blobRepresentation = SIDiffTimestampCompression.getBlobRepresentation();

        var decodedTimestamps = BlobDecompressor.decompressTimestamps(TimestampCompressionModelType.SIDIFF,
                blobRepresentation, dataPoints.get(0).timestamp(), dataPoints.get(dataPoints.size() - 1).timestamp());

        assertEquals(0, decodedTimestamps.get(0));
        assertEquals(2500, decodedTimestamps.get(1)); // 2500 + 250 -> 2500, should get reduced to bucket 0.
        assertEquals(5000 + 251, decodedTimestamps.get(2)); // 5000 + 251 -> 5251, because it is above the threshold for bucket 0 and should stay in bucket 1.
        assertEquals(7500 + 511, decodedTimestamps.get(3)); // 7500 + (511 + 250) -> 7500 + 511, bucket 1 size is 511 so we want to be 511+250 away to get reduced to it its max value
        assertEquals(10000, decodedTimestamps.get(4)); // 10000 -> 10000 because it is precisely reconstructed using the calculated SI.
    }

    @Test
    public void testDecompressingFallbackModels(){
        long timestamp = 1000L;
        float value = 99F;
        DataPoint dataPoint = new DataPoint(timestamp, value);

        TimestampCompressionModel timeModel = new FallbackTimeStampCompressionModel(timestamp);
        ValueCompressionModel valueModel =  new FallbackValueCompressionModel(value);

        List<Long> decompressedTimestamps = BlobDecompressor.decompressTimestamps(timeModel.getTimestampCompressionModelType(),
                timeModel.getBlobRepresentation(),
                timestamp,
                timestamp);
        assertEquals(1, decompressedTimestamps.size());
        assertEquals(timestamp, decompressedTimestamps.get(0));

        List<DataPoint> decompressedDataPoints = BlobDecompressor.createDataPointsByDecompressingValues(valueModel.getValueCompressionModelType(),
                valueModel.getBlobRepresentation(),
                decompressedTimestamps);

        assertEquals(1, decompressedDataPoints.size());
        assertEquals(dataPoint, decompressedDataPoints.get(0));
    }

}