package segmentgenerator;

import records.Segment;
import compression.timestamp.TimeStampCompressionModel;
import compression.value.ValueCompressionModel;
import records.DataPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

public class SegmentGenerator {

    private CompressionModelManager compressionModelManager;
    private String timeSeriesKey;
    private List<DataPoint> notYetEmitted;

    public SegmentGenerator(List<ValueCompressionModel> valueCompressionModels, List<TimeStampCompressionModel> timeStampCompressionModels, String timeSeriesKey) {
        this.compressionModelManager = new CompressionModelManager(valueCompressionModels, timeStampCompressionModels);
        this.timeSeriesKey = timeSeriesKey;
        this.notYetEmitted = new ArrayList<>();
    }

    // TODO: Test this method
    public Optional<Segment> acceptDataPoint(DataPoint dataPoint) {
        boolean appendSuccess = compressionModelManager.tryAppendDataPointToAllModels(dataPoint);

        this.notYetEmitted.add(dataPoint);

        if (appendSuccess) {
            return Optional.empty();
        } else {
            Segment segment = generateSegment();

            //REMOVE EMITTED POINT FROM NOT_YET_EMITTED
            // segment.numberOfDataPoints()

            boolean success = compressionModelManager.resetAndTryAppendBuffer(notYetEmitted);
            if (!success) {
                throw new RuntimeException("We have hit an edge case where more than one segment must be generated to accomedate the new data point");
            }

            return Optional.of(segment);
        }
    }

    private Segment generateSegment() {
        long startTime = notYetEmitted.get(0).timestamp();
        return new Segment(null, 0, 0, 1, null, 1, null);
    }



}