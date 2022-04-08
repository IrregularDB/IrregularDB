package compression.timestamp;

import compression.encoding.SingleIntEncoding;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class RegularTimestampCompressionModelTest {
    RegularTimestampCompressionModel regularModel;

    // Helper method needed to be able to use reset and append all as it now takes data points
    private List<DataPoint> createDataPointsFromTimestamps(List<Long> timestamps) {
        List<DataPoint> dataPoints = new ArrayList<>();
        for (Long timestamp : timestamps) {
            dataPoints.add(createDataPointForTimestamp(timestamp));
        }
        return dataPoints;
    }

    private DataPoint createDataPointForTimestamp(long timeStamp) {
        // We use -1 for our data points as value because this model does not care about the values of the data points
        return new DataPoint(timeStamp, -1);
    }

    @BeforeEach
    void init() {
        regularModel = new RegularTimestampCompressionModel(0);
    }

    // We expect to be able to append any two data points no matter how different as then we have not SI
    @Test
    void appendTwoTimestamps() {
        Assertions.assertTrue(regularModel.append(createDataPointForTimestamp(0L)));
        Assertions.assertTrue(regularModel.append(createDataPointForTimestamp(1000000L)));
    }

    @Test
    void appendThreeTimestampsWithSameSI() {
        Assertions.assertTrue(regularModel.append(createDataPointForTimestamp(0L)));
        Assertions.assertTrue(regularModel.append(createDataPointForTimestamp(100L)));
        Assertions.assertTrue(regularModel.append(createDataPointForTimestamp(200L)));
    }

    @Test
    void appendThreeTimestampsDifferentSI() {
        Assertions.assertTrue(regularModel.append(createDataPointForTimestamp(0L)));
        Assertions.assertTrue(regularModel.append(createDataPointForTimestamp(100L)));
        Assertions.assertFalse(regularModel.append(createDataPointForTimestamp(999L)));
    }

    @Test
    void appendAfterFailedAppendNotAllowed() {
        Assertions.assertTrue(regularModel.append(createDataPointForTimestamp(0L)));
        Assertions.assertTrue(regularModel.append(createDataPointForTimestamp(100L)));
        Assertions.assertFalse(regularModel.append(createDataPointForTimestamp(999L)));

        Assertions.assertThrows(IllegalArgumentException.class, () -> regularModel.append(createDataPointForTimestamp(200L)));
    }

    @Test
    void getLength() {
        Assertions.assertEquals(0, regularModel.getLength());
        regularModel.append(createDataPointForTimestamp(0L));
        Assertions.assertEquals(1, regularModel.getLength());
        regularModel.append(createDataPointForTimestamp(100L));
        regularModel.append(createDataPointForTimestamp(200L));
        Assertions.assertEquals(3, regularModel.getLength());
    }

    @Test
    void resetAndAppendAllEmptyModel() {
        // We test that we can insert three legal points at once on an empty model
        List<Long> timestamps = Arrays.asList(0L, 100L, 200L, 300L);
        Assertions.assertTrue(regularModel.resetAndAppendAll(createDataPointsFromTimestamps(timestamps)));
    }

    @Test
    void resetAndAppendAllNonEmptyModel() {
        // We test that we can insert three legal points at once even though we inserted something different earlier
        // as this call should also reset the model
        List<Long> timestamps = Arrays.asList(0L, 100L, 200L, 300L);
        regularModel.resetAndAppendAll(createDataPointsFromTimestamps(timestamps));

        timestamps = Arrays.asList(0L, 200L, 400L, 600L);
        Assertions.assertTrue(regularModel.resetAndAppendAll(createDataPointsFromTimestamps(timestamps)));
    }

    @Test
    void resetAndAppendAllWhereSomePointCannotBeRepresented() {
        List<Long> timestamps = Arrays.asList(0L, 100L, 200L, 300L, 999L, 999L);
        Assertions.assertFalse(regularModel.resetAndAppendAll(createDataPointsFromTimestamps(timestamps)));
        Assertions.assertEquals(4, regularModel.getLength());
    }

    @Test
    void getTimestampBlobEmptyModel() {
        Assertions.assertThrows(IllegalStateException.class, () -> regularModel.getBlobRepresentation());
    }

    @Test
    void getTimestampBlob() {
        List<Long> timestamps = Arrays.asList(0L, 100L, 200L, 300L);
        regularModel.resetAndAppendAll(createDataPointsFromTimestamps(timestamps));

        ByteBuffer timestampBlob = regularModel.getBlobRepresentation();
        int si = SingleIntEncoding.decode(timestampBlob);
        Assertions.assertEquals(100, si);
    }

    @Test
    void getAmountOfBytesUsed0DataPoints() {
        // We expect this to throw and exception as no model has been made yet.
        Assertions.assertThrows(IllegalStateException.class, () -> regularModel.getAmountBytesUsed());
    }

    @Test
    void getAmountOfBytesUsed() {
        List<Long> timestamps = Arrays.asList(0L, 100L);
        regularModel.resetAndAppendAll(createDataPointsFromTimestamps(timestamps));
        // We expect that we use 1 byte to store 100
        Assertions.assertEquals(1, regularModel.getAmountBytesUsed());
    }

    @Test
    void reduceToSizeN() {
        List<Long> timestamps = Arrays.asList(0L, 100L, 200L, 300L);
        regularModel.resetAndAppendAll(createDataPointsFromTimestamps(timestamps));
        regularModel.reduceToSizeN(2);
        Assertions.assertEquals(2, regularModel.getLength());
    }

    @Test
    void reduceToSizeNIllegalArgument() {
        List<Long> timestamps = Arrays.asList(0L, 100L, 200L, 300L);
        regularModel.resetAndAppendAll(createDataPointsFromTimestamps(timestamps));
        Assertions.assertThrows(IllegalArgumentException.class, () ->  regularModel.reduceToSizeN(5));
    }


    @Test
    void sunshineThresholdTest(){
        List<Long> integers = List.of(100L, 205L, 300L);

        this.regularModel = new RegularTimestampCompressionModel(10);
        boolean success = regularModel.resetAndAppendAll(createDataPointsFromTimestamps(integers));

        Assertions.assertTrue(success);
    }

    @Test
    void sunshineErrorTest(){
        List<Long> integers = List.of(100L, 205L, 310L, 1000L);

        this.regularModel = new RegularTimestampCompressionModel(10);
        boolean success = regularModel.resetAndAppendAll(createDataPointsFromTimestamps(integers));

        Assertions.assertFalse(success);
    }

    @Test
    void sunshineErrorTestWithHigherThreshold(){
        List<Long> integers = List.of(100L, 205L, 310L, 1000L);

        this.regularModel = new RegularTimestampCompressionModel(10000);
        boolean success = regularModel.resetAndAppendAll(createDataPointsFromTimestamps(integers));

        Assertions.assertTrue(success);
    }
}