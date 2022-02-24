package segmentgenerator;

import compression.timestamp.RegularTimeStampCompressionModel;
import compression.timestamp.TimeStampCompressionModelType;
import compression.value.PMCMeanValueCompressionModel;
import compression.value.ValueCompressionModelType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import records.DataPoint;
import records.Segment;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class SegmentGeneratorTest {

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
        Segment expectedSegment = new Segment(1, 1, 6, ValueCompressionModelType.PMC_MEAN.ordinal(), constructValueDataBlob(allDataPoints), TimeStampCompressionModelType.REGULAR.ordinal(), constructTimestampDataBlob(allDataPoints));

        float errorBound = 0;
        TestCompressionModelManagerRegularPMCMean testCompressionModelManagerRegularPMCMean = new TestCompressionModelManagerRegularPMCMean(List.of(new PMCMeanValueCompressionModel(errorBound)), List.of(new RegularTimeStampCompressionModel(errorBound)));

        SegmentGenerator segmentGenerator = new SegmentGenerator(testCompressionModelManagerRegularPMCMean, 1);

        long amountSuccess = dataPoints.stream()
                .map(segmentGenerator::acceptDataPoint)
                .filter(a -> a)
                .count();

        boolean isLastDataPointAccepted = segmentGenerator.acceptDataPoint(lastDataPoint);

        Segment actualSegment = segmentGenerator.constructSegmentFromBuffer();

        Assertions.assertEquals(dataPoints.size(), amountSuccess);
        Assertions.assertFalse(isLastDataPointAccepted);
        Assertions.assertEquals(expectedSegment, actualSegment);
    }

    private ByteBuffer constructValueDataBlob(List<DataPoint> dataPoints){
        PMCMeanValueCompressionModel pmcMeanValueCompressionModel = new PMCMeanValueCompressionModel(0);
        pmcMeanValueCompressionModel.resetAndAppendAll(dataPoints);
        return pmcMeanValueCompressionModel.getBlobRepresentation();
    }

    private ByteBuffer constructTimestampDataBlob(List<DataPoint> dataPoints) {
        RegularTimeStampCompressionModel regularTimeStampCompressionModel = new RegularTimeStampCompressionModel(0);
        regularTimeStampCompressionModel.resetAndAppendAll(dataPoints);
        return regularTimeStampCompressionModel.getBlobRepresentation();
    }
}