package segmentgenerator;

import compression.BlobDecompressor;
import config.ConfigProperties;
import records.*;

import java.util.ArrayList;
import java.util.List;

public class SegmentGenerator {
    private final CompressionModelManager compressionModelManager;
    private final int timeSeriesId;
    private final ArrayList<DataPoint> notYetEmitted;
    private static final boolean usesSegmentSummary = ConfigProperties.getInstance().populateSegmentSummary();
    private long previousAppendedTimestamp;


    public SegmentGenerator(CompressionModelManager compressionModelManager, int timeSeriesId) {
        this.compressionModelManager = compressionModelManager;
        this.timeSeriesId = timeSeriesId;
        this.notYetEmitted = new ArrayList<>();
        this.previousAppendedTimestamp = Long.MIN_VALUE;
    }

    /**
     *
     * @param dataPoint to be appended to models
     * @return on false as the return value, generateSegment must be the next method invoked by the caller
     */
    public boolean acceptDataPoint(DataPoint dataPoint) {
        DataPoint movedDataPoint = checkAndMoveDataPoint(dataPoint, this.previousAppendedTimestamp);
        DataPoint temp;
        if (movedDataPoint != null) {
            temp = movedDataPoint;
            this.previousAppendedTimestamp = movedDataPoint.timestamp();
        } else {
            temp = dataPoint;
        }
        notYetEmitted.add(temp);
        return compressionModelManager.tryAppendDataPointToAllModels(temp);
    }

    private DataPoint checkAndMoveDataPoint(DataPoint dataPoint, Long previousAppendedTimestamp) {
        DataPoint temp = null;
        if (dataPoint.timestamp() <= previousAppendedTimestamp) {
            temp = new DataPoint(previousAppendedTimestamp + 1, dataPoint.value());
        }
        return temp;
    }

    public List<Segment> constructSegmentsFromBuffer() {
        if (this.notYetEmitted.size() == 0) {
            return null;
        }
        List<Segment> segments = new ArrayList<>();
        int amtDataPointsUsed;
        long endTimeOfSegment;

        do {
            CompressionModel bestCompressionModel;
            if (compressionModelManager.canCreateCompressionModel()) {
                bestCompressionModel = this.compressionModelManager.getBestCompressionModel();
            } else {
                DataPoint dataPoint = notYetEmitted.get(0);
                bestCompressionModel = ModelPicker.createFallBackCompressionModel(dataPoint);
            }

            Segment segment = generateSegment(bestCompressionModel, notYetEmitted.get(0).timestamp());

            segments.add(segment);
            amtDataPointsUsed = bestCompressionModel.length();
            endTimeOfSegment = segment.segmentKey().startTime() + segment.endTime();
        } while (!prepareForNextSegment(amtDataPointsUsed, endTimeOfSegment));

        return segments;
    }


    /**
     * @return if this method returns false then another segment must be generated.
     */
    private boolean prepareForNextSegment(int amountOfDataPointsUsedInSegment, long endTimeOfSegment) {
        removeNOldestFromBuffer(amountOfDataPointsUsedInSegment);
        long previousTimestamp = endTimeOfSegment;
        for (int i = 0; i < notYetEmitted.size(); i++) {
            DataPoint movedDataPoint = checkAndMoveDataPoint(notYetEmitted.get(i), previousTimestamp);
            if (movedDataPoint != null) {
                notYetEmitted.set(i, movedDataPoint);
                previousTimestamp = movedDataPoint.timestamp();
            } else {
                break;
            }
        }
        return compressionModelManager.resetAndTryAppendBuffer(notYetEmitted);
    }

    private void removeNOldestFromBuffer(int dataPointsUsedForPrevSegment) {
        notYetEmitted.subList(0, dataPointsUsedForPrevSegment).clear();
    }

    private Segment generateSegment(CompressionModel compressionModel, long startTime) {
        SegmentKey segmentKey = new SegmentKey(this.timeSeriesId, startTime);
        SegmentSummary segmentSummary = null;

        List<Long> decompressedTimestamp = BlobDecompressor.decompressTimestampsUsingAmtDataPoints(
                compressionModel.timestampType(),
                compressionModel.timestampCompressionModel(),
                startTime,
                compressionModel.length()
        );

        if (usesSegmentSummary) {
            List<DataPoint> decompressedDataPoints = BlobDecompressor.createDataPointsByDecompressingValues(
                    compressionModel.valueType(),
                    compressionModel.valueCompressionModel(),
                    decompressedTimestamp
            );
            segmentSummary = new SegmentSummary(decompressedDataPoints, segmentKey);
        }

        return new Segment(
                segmentKey,
                decompressedTimestamp.get(decompressedTimestamp.size() -1),
                (byte) compressionModel.valueType().ordinal(),
                compressionModel.valueCompressionModel(),
                (byte) compressionModel.timestampType().ordinal(),
                compressionModel.timestampCompressionModel(),
                segmentSummary);
    }

}