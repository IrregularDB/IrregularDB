package segmentgenerator;

import config.ConfigProperties;
import records.CompressionModel;
import records.Segment;
import records.DataPoint;

import java.util.ArrayList;
import java.util.List;

public class SegmentGenerator {

    private final CompressionModelManager compressionModelManager;
    private final int timeSeriesId;
    private List<DataPoint> notYetEmitted;

    public SegmentGenerator(CompressionModelManager compressionModelManager, int timeSeriesId) {
        this.compressionModelManager = compressionModelManager;
        this.timeSeriesId = timeSeriesId;
        this.notYetEmitted = new ArrayList<>();
    }

    /**
     *
     * @param dataPoint
     * @return on false as the return value, generateSegment must be the next method invoked by the caller
     */
    public boolean acceptDataPoint(DataPoint dataPoint) {
        notYetEmitted.add(dataPoint);
        boolean appendSuccess = compressionModelManager.tryAppendDataPointToAllModels(dataPoint);

        return appendSuccess;
    }

    public Segment constructSegmentFromBuffer() {
        if (this.notYetEmitted.size() == 0) {
            return null;
        }

        CompressionModel bestCompressionModel = this.compressionModelManager.getBestCompressionModel();

        //find size of each model and reduce the largest down to the size of the smallest
        if (bestCompressionModel.length() == 0) {
            throw new RuntimeException("Segment generated with size 0");
        }

        Segment segment = generateSegment(
                bestCompressionModel,
                notYetEmitted.get(0).timestamp(),
                notYetEmitted.get(bestCompressionModel.length() - 1).timestamp()
        );
        prepareForNextSegment(segment.dataPointsUsed().size());

        return segment;
    }

    private void prepareForNextSegment(int amountOfDataPointsUsedInSegment) {
        removeNOldestFromBuffer(amountOfDataPointsUsedInSegment);
        boolean success = compressionModelManager.resetAndTryAppendBuffer(notYetEmitted);
        if (!success) {
            // TODO: FIX THIS PROBLEM OF HANDLING DATA POINTS WHERE TIME STAMP DIFFERENCE IS LARGER THAN INT_MAX
            //   IT IS NOT AN EDGE CASE
            throw new RuntimeException("We have hit an edge case where more than one segment must be generated to accommodate the new data point");
        }
    }

    private void removeNOldestFromBuffer(int dataPointsUsedForPrevSegment) {
        notYetEmitted.subList(0, dataPointsUsedForPrevSegment).clear();
//        this.notYetEmitted = notYetEmitted.subList(dataPointsUsedForPrevSegment, notYetEmitted.size());
    }

    private Segment generateSegment(CompressionModel compressionModel, long startTime, long endTime) {
        return new Segment(
                this.timeSeriesId,
                startTime,
                endTime,
                (byte) compressionModel.valueType().ordinal(),
                compressionModel.valueCompressionModel(),
                (byte) compressionModel.timestampType().ordinal(),
                compressionModel.timestampCompressionModel(),
                new ArrayList<>(this.notYetEmitted.subList(0, compressionModel.length())) // Should be decompressed values
        );
    }

}