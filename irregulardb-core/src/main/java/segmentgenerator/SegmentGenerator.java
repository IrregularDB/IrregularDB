package segmentgenerator;

import compression.BlobDecompressor;
import compression.timestamp.FallbackTimeStampCompressionModel;
import compression.timestamp.TimestampCompressionModel;
import compression.value.FallbackValueCompressionModel;
import compression.value.ValueCompressionModel;
import compression.value.ValueCompressionModelType;
import config.ConfigProperties;
import records.*;

import java.util.ArrayList;
import java.util.List;

public class SegmentGenerator {
    private final CompressionModelManager compressionModelManager;
    private final int timeSeriesId;
    private final List<DataPoint> notYetEmitted;
    private static final boolean usesSegmentSummary = ConfigProperties.getInstance().populateSegmentSummary();


    public SegmentGenerator(CompressionModelManager compressionModelManager, int timeSeriesId) {
        this.compressionModelManager = compressionModelManager;
        this.timeSeriesId = timeSeriesId;
        this.notYetEmitted = new ArrayList<>();
    }

    /**
     *
     * @param dataPoint to be appended to models
     * @return on false as the return value, generateSegment must be the next method invoked by the caller
     */
    public boolean acceptDataPoint(DataPoint dataPoint) {
        notYetEmitted.add(dataPoint);
        return compressionModelManager.tryAppendDataPointToAllModels(dataPoint);
    }

    public List<Segment> constructSegmentsFromBuffer() {
        if (this.notYetEmitted.size() == 0) {
            return null;
        }
        List<Segment> segments = new ArrayList<>();
        int amtDataPointsUsed;

        do {
            CompressionModel bestCompressionModel;
            if (notYetEmitted.size() == 1) {
                DataPoint dataPoint = notYetEmitted.get(0);
                bestCompressionModel = getFallBackCompressionModel(dataPoint);
            } else {
                bestCompressionModel = this.compressionModelManager.getBestCompressionModel();
            }

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
            amtDataPointsUsed = bestCompressionModel.length();
        } while (!prepareForNextSegment(amtDataPointsUsed));

        return segments;
    }

    private CompressionModel getFallBackCompressionModel(DataPoint dataPoint) {
        CompressionModel bestCompressionModel;
        TimestampCompressionModel timestampCompressionModel = new FallbackTimeStampCompressionModel(dataPoint.timestamp());
        ValueCompressionModel valueCompressionModel = new FallbackValueCompressionModel(dataPoint.value());
        bestCompressionModel = new CompressionModel(
                valueCompressionModel.getValueCompressionModelType(),
                valueCompressionModel.getBlobRepresentation(),
                timestampCompressionModel.getTimestampCompressionModelType(),
                timestampCompressionModel.getBlobRepresentation(),
                1);
        return bestCompressionModel;
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
        SegmentKey segmentKey = new SegmentKey(this.timeSeriesId, startTime);
        SegmentSummary segmentSummary = null;
        if (usesSegmentSummary) {
            List<DataPoint> decompressedDataPoints = BlobDecompressor.decompressBlobs(compressionModel.timestampType(),
                    compressionModel.timestampCompressionModel(), compressionModel.valueType(),
                    compressionModel.valueCompressionModel(), startTime, endTime);
            segmentSummary = new SegmentSummary(decompressedDataPoints, segmentKey);
        }

        return new Segment(
                segmentKey,
                endTime,
                (byte) compressionModel.valueType().ordinal(),
                compressionModel.valueCompressionModel(),
                (byte) compressionModel.timestampType().ordinal(),
                compressionModel.timestampCompressionModel(),
                segmentSummary);
    }

}