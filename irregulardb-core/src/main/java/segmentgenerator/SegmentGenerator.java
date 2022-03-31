package segmentgenerator;

import compression.CompressionModel;
import records.Segment;
import compression.timestamp.TimeStampCompressionModel;
import compression.value.ValueCompressionModel;
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

    public boolean acceptDataPoint(DataPoint dataPoint) {
        notYetEmitted.add(dataPoint);
        return compressionModelManager.tryAppendDataPointToAllModels(dataPoint);
    }

    public Segment constructSegmentFromBuffer() {
        if (this.notYetEmitted.size() == 0) {
            return null;
        }

        CompressionModel bestCompressionModel = this.compressionModelManager.getBestCompressionModel();

        //find size of each model and reduce the largest down to the size of the smallest
        int amountOfDataPoints = syncValueAndTimeStampModelLength(bestCompressionModel);
        if (amountOfDataPoints == 0) {
            throw new RuntimeException("Segment generated with size 0");
        }

        Segment segment = generateSegment(bestCompressionModel, notYetEmitted.get(0).timestamp(), notYetEmitted.get(amountOfDataPoints - 1).timestamp());

        prepareForNextSegment(segment.dataPointsUsed().size());

        return segment;
    }

    private void prepareForNextSegment(int amountOfDataPointsUsedInSegment) {
        popNFromBuffer(amountOfDataPointsUsedInSegment);
        boolean success = compressionModelManager.resetAndTryAppendBuffer(notYetEmitted);
        if (!success) {
            throw new RuntimeException("We have hit an edge case where more than one segment must be generated to accommodate the new data point");
        }
    }

    private void popNFromBuffer(int dataPointsUsedForPrevSegment) {
        this.notYetEmitted = notYetEmitted.subList(dataPointsUsedForPrevSegment, notYetEmitted.size());
    }

    private int syncValueAndTimeStampModelLength(CompressionModel bestCompressionModel) {
        ValueCompressionModel valueCompressionModel = bestCompressionModel.getValueCompressionModel();
        int valueCompressionModelLength = valueCompressionModel.getLength();

        TimeStampCompressionModel timeStampCompressionModel = bestCompressionModel.getTimeStampCompressionModel();
        int timestampCompressionModelLength = timeStampCompressionModel.getLength();

        if (timestampCompressionModelLength > valueCompressionModelLength) {
            timeStampCompressionModel.reduceToSizeN(valueCompressionModelLength);
        } else {
            valueCompressionModel.reduceToSizeN(timestampCompressionModelLength);
        }
        return Integer.min(timestampCompressionModelLength, valueCompressionModelLength);
    }

    private Segment generateSegment(CompressionModel compressionModel, long startTime, long endTime) {
        ValueCompressionModel valueModel = compressionModel.getValueCompressionModel();
        TimeStampCompressionModel timeStampModel = compressionModel.getTimeStampCompressionModel();

        return new Segment(
                this.timeSeriesId,
                startTime,
                endTime,
                (byte) valueModel.getValueCompressionModelType().ordinal(),
                valueModel.getBlobRepresentation(),
                (byte) timeStampModel.getTimeStampCompressionModelType().ordinal(),
                timeStampModel.getBlobRepresentation(),
                this.notYetEmitted.subList(0, valueModel.getLength())
        );
    }

}