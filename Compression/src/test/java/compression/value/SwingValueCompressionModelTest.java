package compression.value;

import compression.BlobDecompressor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

class SwingValueCompressionModelTest {
    SwingValueCompressionModel swingModel;

    // Helper method needed to be able to use reset and append all as it now takes data points
    private List<DataPoint> createDataPointsFromValues(List<Float> values) {
        List<DataPoint> dataPoints = new ArrayList<>();
        int i = 0;
        for (var value : values) {
            dataPoints.add(new DataPoint(i, value));
            i++;
        }
        return dataPoints;
    }

    @BeforeEach
    void init() {
        float errorBound = 10;
        swingModel = new SwingValueCompressionModel(errorBound);
    }

    @Test
    void appendTwoValues() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F));

        Assertions.assertTrue(swingModel.append(dataPoints.get(0)));
        Assertions.assertTrue(swingModel.append(dataPoints.get(1)));
    }

    @Test
    void appendThreeValues() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F));

        Assertions.assertTrue(swingModel.append(dataPoints.get(0)));
        Assertions.assertTrue(swingModel.append(dataPoints.get(1)));
        Assertions.assertTrue(swingModel.append(dataPoints.get(2)));
    }

    @Test
    void appendVeryDifferentValue() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 9.99F));

        Assertions.assertTrue(swingModel.append(dataPoints.get(0)));
        Assertions.assertTrue(swingModel.append(dataPoints.get(1)));
        Assertions.assertFalse(swingModel.append(dataPoints.get(2)));
        Assertions.assertEquals(2, swingModel.getLength());
    }

    @Test
    void appendAfterFailedAppendNotAllowed() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 9.99F, 1.10F));

        swingModel.append(dataPoints.get(0));
        swingModel.append(dataPoints.get(1));
        swingModel.append(dataPoints.get(2));
        Assertions.assertThrows(IllegalArgumentException.class, () -> swingModel.append(dataPoints.get(3)));
    }

    @Test
    void getLength() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F));

        Assertions.assertEquals(0, swingModel.getLength());
        swingModel.append(dataPoints.get(0));
        Assertions.assertEquals(1, swingModel.getLength());
        swingModel.append(dataPoints.get(1));
        swingModel.append(dataPoints.get(2));
        Assertions.assertEquals(3, swingModel.getLength());
    }

    @Test
    void resetAndAppendAllEmptyModel() {
        // We test what happens when we append to an empty model
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F, 1.14F, 1.21F, 1.28F, 1.32F));

        Assertions.assertTrue(swingModel.resetAndAppendAll(dataPoints));
    }

    @Test
    void resetAndAppendAllNonRegularTimestamps() {
        List<DataPoint> dataPoints = new ArrayList<>();
        dataPoints.add(new DataPoint(0, 1.00F));
        dataPoints.add(new DataPoint(3, 1.03F));
        dataPoints.add(new DataPoint(7, 1.07F));
        dataPoints.add(new DataPoint(17, 1.20F));
        dataPoints.add(new DataPoint(35, 1.40F));

        Assertions.assertTrue(swingModel.resetAndAppendAll(dataPoints));
    }

    @Test
    void resetAndAppendNegativeSlope() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(10.00F, 9.00F, 8.00F, 7.00F, 6.00F, 5.00F));
        Assertions.assertTrue(swingModel.resetAndAppendAll(dataPoints));
    }

    @Test
    void resetAndAppendAllNonEmptyModel() {
        // Here we expect it to be able to append 3 data points even though they are very
        // different compared to the old ones as it should be reset
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F, 1.20F));

        swingModel.resetAndAppendAll(dataPoints);
        dataPoints = createDataPointsFromValues(Arrays.asList(99.9F, 99.9F, 99.9F));
        Assertions.assertTrue(swingModel.resetAndAppendAll(dataPoints));
    }

    @Test
    void resetAndAppendAllWhereSomePointCannotBeRepresented() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F, 1.20F, 99.9F, 99.9F));
        Assertions.assertFalse(swingModel.resetAndAppendAll(dataPoints));
        Assertions.assertEquals(5, swingModel.getLength());
    }

    @Test
    void noErrorBoundAppendAllTest() {
        float errorBound = 0;
        swingModel = new SwingValueCompressionModel(errorBound);
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F, 1.20F));
        Assertions.assertTrue(swingModel.resetAndAppendAll(dataPoints));
    }

    @Test
    void noErrorBoundAppendAllSmallErrorNotAllowedTest() {
        float errorBound = 0;
        swingModel = new SwingValueCompressionModel(errorBound);
        // 1.26 is slightly off from 1.25 thereby not allowed.
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F, 1.20F, 1.26F));
        Assertions.assertFalse(swingModel.resetAndAppendAll(dataPoints));
        Assertions.assertEquals(5, swingModel.getLength());
    }


    @Test
    void effectOfSwingUp() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F));

        swingModel.resetAndAppendAll(dataPoints);
        // The initial lower bound is: -0.05x + 1.
        // So if we did not swing the lower bound up we would be allowed to do the following
        Assertions.assertFalse(swingModel.append(new DataPoint(20, 0)));
    }

    @Test
    void effectOfSwingDown() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F));
        swingModel.resetAndAppendAll(dataPoints);
        // The initial upper bound is: 0.15x + 1.
        // So if we did not swing the lower bound up we would be allowed to do the following
        Assertions.assertFalse(swingModel.append(new DataPoint(20, 4)));

    }


    @Test
    void getValueBlobEmptyModel() {
        Assertions.assertThrows(IllegalStateException.class, () -> swingModel.getBlobRepresentation());
    }

    @Test
    void getValueBlobNonEmptyModel() {
        List<DataPoint> dataPoints = createDataPointsFromValues(Arrays.asList(1.00F, 1.05F, 1.10F, 1.15F, 1.20F));
        swingModel.resetAndAppendAll(dataPoints);
        ByteBuffer valueBlob = swingModel.getBlobRepresentation();
        var slope = valueBlob.getFloat(0);
        var intercept = valueBlob.getFloat(4);

        var allowedDifference = 0.000000000001;
        Assertions.assertTrue(0.05F - slope < allowedDifference);
        Assertions.assertTrue(1.00F - intercept < allowedDifference);
    }

    @Test
    void getAmountOfBytesUsed0DataPoints() {
        // We expect this to throw and exception as no model has been made yet.
        Assertions.assertThrows(IllegalStateException.class, () -> swingModel.getAmountBytesUsed());
    }

    @Test
    void getAmountOfBytesUsed() {
        var values = Arrays.asList(1.00F, 1.00F);
        swingModel.resetAndAppendAll(createDataPointsFromValues(values));
        // We expect that we use 8 bytes (i.e. 2 floats)
        Assertions.assertEquals(8, swingModel.getAmountBytesUsed());
    }

    @Test
    void reduceToSizeN() {
        var values = Arrays.asList(1.0F, 1.0F, 1.0F, 1.0F);
        swingModel.resetAndAppendAll(createDataPointsFromValues(values));
        swingModel.reduceToSizeN(2);
        Assertions.assertEquals(2, swingModel.getLength());
    }

    @Test
    void reduceToSizeNIllegalArgument() {
        var values = Arrays.asList(1.0F, 1.0F, 1.0F, 1.0F);
        swingModel.resetAndAppendAll(createDataPointsFromValues(values));
        Assertions.assertThrows(IllegalArgumentException.class, () ->  swingModel.reduceToSizeN(5));
    }

    @Test
    void specificTest(){
        List<String> stringReadings = List.of(
                "1304136460000 2.00",
                "1304136463000 2.00",
                "1304136472000 0.00",
                "1304136476000 0.00",
                "1304136479000 0.00",
                "1304136482000 0.00",
                "1304136486000 0.00",
                "1304136489000 0.00",
                "1304136493000 0.00",
                "1304136496000 0.00",
                "1304136500000 0.00",
                "1304136503000 0.00",
                "1304136507000 0.00",
                "1304136510000 0.00",
                "1304136514000 0.00",
                "1304136517000 0.00",
                "1304136521000 0.00",
                "1304136524000 0.00",
                "1304136533000 0.00"
        );

        List<DataPoint> dataPoints = stringReadings.stream().map(this::stringToDataPoint).toList();

        boolean success = swingModel.resetAndAppendAll(dataPoints);

        Assertions.assertFalse(success);
        Assertions.assertEquals(2, swingModel.getLength());
    }


    @Test
    void anotherSpecificTest(){
        List<String> strings = List.of(
        "1304870473000 12.00",
        "1304870476000 12.00",
        "1304870480000 12.00",
        "1304870483000 12.00",
        "1304870487000 12.00",
        "1304870490000 12.00",
        "1304870494000 12.00",
        "1304870497000 12.00",
        "1304870501000 12.00",
        "1304870510000 12.00",
        "1304870513000 12.00",
        "1304870517000 12.00",
        "1304870520000 12.00",
        "1304870524000 12.00",
        "1304870527000 12.00",
        "1304870531000 12.00",
        "1304870534000 12.00",
        "1304870538000 13.00",
        "1304870542000 12.00",
        "1304870545000 12.00",
        "1304870549000 12.00",
        "1304870552000 12.00",
        "1304870556000 12.00",
        "1304870559000 12.00",
        "1304870563000 12.00",
        "1304870572000 12.00",
        "1304870575000 12.00",
        "1304870579000 12.00",
        "1304870582000 12.00",
        "1304870586000 12.00",
        "1304870589000 12.00",
        "1304870592000 12.00",
        "1304870596000 12.00",
        "1304870599000 12.00",
        "1304870603000 12.00",
        "1304870606000 13.00",
        "1304870610000 12.00",
        "1304870613000 12.00",
        "1304870617000 12.00",
        "1304870620000 12.00",
        "1304870629000 12.00",
        "1304870632000 12.00",
        "1304870636000 13.00",
        "1304870639000 13.00",
        "1304870643000 12.00",
        "1304870646000 12.00",
        "1304870650000 13.00",
        "1304870653000 12.00",
        "1304870657000 12.00",
        "1304870660000 11.00"
        );

        List<DataPoint> dataPoints = strings.stream().map(this::stringToDataPoint).toList();
        List<Long> timstamps = dataPoints.stream().map(DataPoint::timestamp).toList();

        boolean b = swingModel.resetAndAppendAll(dataPoints);

        ByteBuffer blobRepresentation = swingModel.getBlobRepresentation();

        List<DataPoint> decompressedDataPoints = BlobDecompressor.createDataPointsByDecompressingValues(ValueCompressionModelType.SWING, blobRepresentation, timstamps);
        int a =1;


    }

    private DataPoint stringToDataPoint(String str) {
        String[] s = str.split(" ");
        return new DataPoint(Long.parseLong(s[0]), Float.parseFloat(s[1]));
    }
}