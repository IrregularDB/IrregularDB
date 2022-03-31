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

class RegularTimeStampCompressionModelTest {
    RegularTimestampCompressionModel regularModel;

    // Helper method needed to be able to use reset and append all as it now takes data points
    private List<DataPoint> createDataPointsFromTimeStamps(List<Long> timeStamps) {
        List<DataPoint> dataPoints = new ArrayList<>();
        for (Long timeStamp : timeStamps) {
            dataPoints.add(createDataPointForTimeStamp(timeStamp));
        }
        return dataPoints;
    }

    private DataPoint createDataPointForTimeStamp(long timeStamp) {
        // We use -1 for our data points as value because this model does not care about the values of the data points
        return new DataPoint(timeStamp, -1);
    }

    @BeforeEach
    void init() {
        regularModel = new RegularTimestampCompressionModel(0);
    }

    // We expect to be able to append any two data points no matter how different as then we have not SI
    @Test
    void appendTwoTimeStamps() {
        Assertions.assertTrue(regularModel.append(createDataPointForTimeStamp(0L)));
        Assertions.assertTrue(regularModel.append(createDataPointForTimeStamp(1000000L)));
    }

    @Test
    void appendThreeTimeStampsWithSameSI() {
        Assertions.assertTrue(regularModel.append(createDataPointForTimeStamp(0L)));
        Assertions.assertTrue(regularModel.append(createDataPointForTimeStamp(100L)));
        Assertions.assertTrue(regularModel.append(createDataPointForTimeStamp(200L)));
    }

    @Test
    void appendThreeTimeStampsDifferentSI() {
        Assertions.assertTrue(regularModel.append(createDataPointForTimeStamp(0L)));
        Assertions.assertTrue(regularModel.append(createDataPointForTimeStamp(100L)));
        Assertions.assertFalse(regularModel.append(createDataPointForTimeStamp(999L)));
    }

    @Test
    void appendAfterFailedAppendNotAllowed() {
        Assertions.assertTrue(regularModel.append(createDataPointForTimeStamp(0L)));
        Assertions.assertTrue(regularModel.append(createDataPointForTimeStamp(100L)));
        Assertions.assertFalse(regularModel.append(createDataPointForTimeStamp(999L)));

        Assertions.assertThrows(IllegalArgumentException.class, () -> regularModel.append(createDataPointForTimeStamp(200L)));
    }

    @Test
    void getLength() {
        Assertions.assertEquals(0, regularModel.getLength());
        regularModel.append(createDataPointForTimeStamp(0L));
        Assertions.assertEquals(1, regularModel.getLength());
        regularModel.append(createDataPointForTimeStamp(100L));
        regularModel.append(createDataPointForTimeStamp(200L));
        Assertions.assertEquals(3, regularModel.getLength());
    }

    @Test
    void resetAndAppendAllEmptyModel() {
        // We test that we can insert three legal points at once on an empty model
        List<Long> timeStamps = Arrays.asList(0L, 100L, 200L, 300L);
        Assertions.assertTrue(regularModel.resetAndAppendAll(createDataPointsFromTimeStamps(timeStamps)));
    }

    @Test
    void resetAndAppendAllNonEmptyModel() {
        // We test that we can insert three legal points at once even though we inserted something different earlier
        // as this call should also reset the model
        List<Long> timeStamps = Arrays.asList(0L, 100L, 200L, 300L);
        regularModel.resetAndAppendAll(createDataPointsFromTimeStamps(timeStamps));

        timeStamps = Arrays.asList(0L, 200L, 400L, 600L);
        Assertions.assertTrue(regularModel.resetAndAppendAll(createDataPointsFromTimeStamps(timeStamps)));
    }

    @Test
    void resetAndAppendAllWhereSomePointCannotBeRepresented() {
        List<Long> timeStamps = Arrays.asList(0L, 100L, 200L, 300L, 999L, 999L);
        Assertions.assertFalse(regularModel.resetAndAppendAll(createDataPointsFromTimeStamps(timeStamps)));
        Assertions.assertEquals(4, regularModel.getLength());
    }

    @Test
    void getTimeStampBlobEmptyModel() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> regularModel.getBlobRepresentation());
    }

    @Test
    void getTimeStampBlob() {
        List<Long> timeStamps = Arrays.asList(0L, 100L, 200L, 300L);
        regularModel.resetAndAppendAll(createDataPointsFromTimeStamps(timeStamps));

        ByteBuffer timeStampBlob = regularModel.getBlobRepresentation();
        int si = SingleIntEncoding.decode(timeStampBlob);
        Assertions.assertEquals(100, si);
    }

    @Test
    void getAmountOfBytesUsed0DataPoints() {
        // We expect this to throw and exception as no model has been made yet.
        Assertions.assertThrows(UnsupportedOperationException.class, () -> regularModel.getAmountBytesUsed());
    }

    @Test
    void getAmountOfBytesUsed() {
        List<Long> timeStamps = Arrays.asList(0L, 100L);
        regularModel.resetAndAppendAll(createDataPointsFromTimeStamps(timeStamps));
        // We expect that we use 1 byte to store 100
        Assertions.assertEquals(1, regularModel.getAmountBytesUsed());
    }

    @Test
    void reduceToSizeN() {
        List<Long> timeStamps = Arrays.asList(0L, 100L, 200L, 300L);
        regularModel.resetAndAppendAll(createDataPointsFromTimeStamps(timeStamps));
        regularModel.reduceToSizeN(2);
        Assertions.assertEquals(2, regularModel.getLength());
    }

    @Test
    void reduceToSizeNIllegalArgument() {
        List<Long> timeStamps = Arrays.asList(0L, 100L, 200L, 300L);
        regularModel.resetAndAppendAll(createDataPointsFromTimeStamps(timeStamps));
        Assertions.assertThrows(IllegalArgumentException.class, () ->  regularModel.reduceToSizeN(5));
    }


    @Test
    void sunshineThresholdTest(){
        List<Long> integers = List.of(100L, 205L, 300L);

        this.regularModel = new RegularTimestampCompressionModel(10);
        boolean success = regularModel.resetAndAppendAll(createDataPointsFromTimeStamps(integers));

        Assertions.assertTrue(success);
    }

    @Test
    void sunshineErrorTest(){
        List<Long> integers = List.of(100L, 205L, 310L, 1000L);

        this.regularModel = new RegularTimestampCompressionModel(10);
        boolean success = regularModel.resetAndAppendAll(createDataPointsFromTimeStamps(integers));

        Assertions.assertFalse(success);
    }

    @Test
    void sunshineErrorTestWithHigherThreshold(){
        List<Long> integers = List.of(100L, 205L, 310L, 1000L);

        this.regularModel = new RegularTimestampCompressionModel(10000);
        boolean success = regularModel.resetAndAppendAll(createDataPointsFromTimeStamps(integers));

        Assertions.assertTrue(success);
    }
}