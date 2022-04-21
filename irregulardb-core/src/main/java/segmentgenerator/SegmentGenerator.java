package segmentgenerator;

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

    public List<Segment> constructSegmentsFromBuffer() {
        if (this.notYetEmitted.size() == 0) {
            return null;
        }
        List<Segment> segments = new ArrayList<>();
        int amtDataPointsUsed;

        do {
            CompressionModel bestCompressionModel = this.compressionModelManager.getBestCompressionModel();

            //find size of each model and reduce the largest down to the size of the smallest
            if (bestCompressionModel.length() == 0) {
                throw new RuntimeException("Segment generated with size 0");
            }

            Segment segment = generateSegment(
                    bestCompressionModel,
                    notYetEmitted.get(0).timestamp(),
                    notYetEmitted.get(bestCompressionModel.length()- 1).timestamp()
            );
            segments.add(segment);
            amtDataPointsUsed = segment.dataPointsUsed().size();
        } while (!prepareForNextSegment(amtDataPointsUsed));

        return segments;
    }

    /**
     * @return if this method returns false then another segment must be generated.
     */
    private boolean prepareForNextSegment(int amountOfDataPointsUsedInSegment) {
        removeNOldestFromBuffer(amountOfDataPointsUsedInSegment);
        return compressionModelManager.resetAndTryAppendBuffer(notYetEmitted);
    }

    private void removeNOldestFromBuffer(int dataPointsUsedForPrevSegment) {
        notYetEmitted.subList(0, dataPointsUsedForPrevSegment).clear();
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
                new ArrayList<>(this.notYetEmitted.subList(0, compressionModel.length())) // TODO: Should be decompressed values
        );
    }

}