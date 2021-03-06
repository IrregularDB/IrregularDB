package segmentgenerator;

import compression.timestamp.RegularTimestampCompressionModel;
import compression.timestamp.TimestampCompressionModelType;
import compression.value.PMCMeanValueCompressionModel;
import compression.value.ValueCompressionModelType;
import config.ConfigProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import records.DataPoint;
import records.Segment;
import records.SegmentKey;
import records.SegmentSummary;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

class SegmentGeneratorTest {
    @BeforeAll
    public static void setupConfig(){
        ConfigProperties.isTest = true;
    }

    @Test
    public void testSegmentGeneratorAcceptsPmcMeanDataPoints() {

        // Make some data points that forms a PMC-mean segment and a last point that 'breaks' the segment
        List<DataPoint> dataPoints = List.of(
                new DataPoint(1, 1),
                new DataPoint(2, 1),
                new DataPoint(3, 1),
                new DataPoint(4, 1),
                new DataPoint(5, 1),
                new DataPoint(6, 1)
        );
        DataPoint lastDataPoint = new DataPoint(7, 1000);

        List<DataPoint> allDataPoints = new ArrayList<>(dataPoints);
        allDataPoints.add(lastDataPoint);
        SegmentKey segmentKey = new SegmentKey(1, 1);
        Segment expectedSegment = new Segment(
                segmentKey,
                6,
                (byte) ValueCompressionModelType.PMC_MEAN.ordinal(),
                constructValueDataBlob(allDataPoints),
                (byte) TimestampCompressionModelType.REGULAR.ordinal(),
                constructTimestampDataBlob(allDataPoints),
                new SegmentSummary(dataPoints, segmentKey)
        );

        ModelPicker modelPicker = ModelPickerFactory.createModelPickerFromConfig();
        TestCompressionModelManagerRegularPMCMean testCompressionModelManagerRegularPMCMean = new TestCompressionModelManagerRegularPMCMean(List.of(new PMCMeanValueCompressionModel(0)), List.of(new RegularTimestampCompressionModel(0)), modelPicker);

        SegmentGenerator segmentGenerator = new SegmentGenerator(testCompressionModelManagerRegularPMCMean, 1);

        long amountSuccess = dataPoints.stream()
                .map(segmentGenerator::acceptDataPoint)
                .filter(a -> a)
                .count();

        boolean isLastDataPointAccepted = segmentGenerator.acceptDataPoint(lastDataPoint);

        List<Segment> segments = segmentGenerator.constructSegmentsFromBuffer();
        Segment segment = segments.get(0);


        Assertions.assertEquals(dataPoints.size(), amountSuccess);
        Assertions.assertFalse(isLastDataPointAccepted);
        Assertions.assertEquals(expectedSegment, segment);
    }

    @Test
    public void testLengthBound(){
        int EXPECTED_LENGTH_BOUND = 50; //this is defined in the test config.properties
        List<DataPoint> nPmcMeanDataPoints = getNPmcMeanDataPoints(1, 1, 1, EXPECTED_LENGTH_BOUND);

        ModelPicker modelPicker = ModelPickerFactory.createModelPickerFromConfig();
        TestCompressionModelManagerRegularPMCMean testCompressionModelManagerRegularPMCMean = new TestCompressionModelManagerRegularPMCMean(List.of(new PMCMeanValueCompressionModel(0)), List.of(new RegularTimestampCompressionModel(0)), modelPicker);

        SegmentGenerator segmentGenerator = new SegmentGenerator(testCompressionModelManagerRegularPMCMean, 1);

        long amountAcceptedDataPoints = nPmcMeanDataPoints.stream()
                .filter(segmentGenerator::acceptDataPoint)
                .count();

        Assertions.assertEquals(nPmcMeanDataPoints.size(), amountAcceptedDataPoints);
    }


    @Test
    void testSingleDatapoint() {
        DataPoint dataPoint = new DataPoint(1000L, 99F);
        ModelPicker modelPicker = ModelPickerFactory.createModelPickerFromConfig();
        TestCompressionModelManagerRegularPMCMean testCompressionModelManagerRegularPMCMean = new TestCompressionModelManagerRegularPMCMean(List.of(new PMCMeanValueCompressionModel(0)), List.of(new RegularTimestampCompressionModel(0)), modelPicker);

        SegmentGenerator segmentGenerator = new SegmentGenerator(testCompressionModelManagerRegularPMCMean, 1);
        segmentGenerator.acceptDataPoint(dataPoint);

        List<Segment> segments = segmentGenerator.constructSegmentsFromBuffer();
        Assertions.assertEquals(1, segments.size());
        Segment segment = segments.get(0);
        // We expect it to use the fall back model type
        byte expectedTimestampModelType = (byte) TimestampCompressionModelType.FALLBACK.ordinal();
        Assertions.assertEquals(expectedTimestampModelType, segment.timestampModelType());

        byte expectedValueModelType = (byte) ValueCompressionModelType.FALLBACK.ordinal();
        Assertions.assertEquals(expectedValueModelType, segment.valueModelType());
    }


    private List<DataPoint> getNPmcMeanDataPoints(int startTime, int timeIncrement, float value, int amount) {
        List<DataPoint> dataPoints = new ArrayList<>();
        int time = startTime;
        for (int i = 0; i < amount; i++) {
            dataPoints.add(new DataPoint(time, value));
            time += timeIncrement;
        }
        return dataPoints;
    }

    private ByteBuffer constructValueDataBlob(List<DataPoint> dataPoints){
        PMCMeanValueCompressionModel pmcMeanValueCompressionModel = new PMCMeanValueCompressionModel(0);
        pmcMeanValueCompressionModel.resetAndAppendAll(dataPoints);
        return pmcMeanValueCompressionModel.getBlobRepresentation();
    }

    private ByteBuffer constructTimestampDataBlob(List<DataPoint> dataPoints) {
        RegularTimestampCompressionModel regularTimestampCompressionModel = new RegularTimestampCompressionModel(0);
        regularTimestampCompressionModel.resetAndAppendAll(dataPoints);
        return regularTimestampCompressionModel.getBlobRepresentation();
    }
}