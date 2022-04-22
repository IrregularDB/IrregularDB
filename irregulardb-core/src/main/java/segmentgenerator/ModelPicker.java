package segmentgenerator;

import compression.BaseModel;
import config.ConfigProperties;
import records.CompressionModel;
import compression.timestamp.TimestampCompressionModel;
import compression.value.ValueCompressionModel;

import java.util.Comparator;
import java.util.List;

public abstract class ModelPicker {

    protected static final int overheadPerModel = calculateOverheadPerModel();

    public abstract CompressionModel findBestCompressionModel(List<ValueCompressionModel> valueCompressionModels, List<TimestampCompressionModel> timestampCompressionModels);

    protected double calculateAmountBytesPerDataPoint(int bytesUsedByModel, int amountDataPoints) {
        int amountBytesUsed = overheadPerModel + bytesUsedByModel;
        return ((double) amountBytesUsed) / ((double) amountDataPoints);
    }

    protected double calculateAmountBytesPerDataPoint(BaseModel baseModel) {
        if (baseModel.canCreateByteBuffer()) {
            int amountBytesUsedForModel = baseModel.getAmountBytesUsed();
            return calculateAmountBytesPerDataPoint(amountBytesUsedForModel, baseModel.getLength());
        } else {
            return Double.MAX_VALUE;
        }
    }

    private static int calculateOverheadPerModel() {
        // TODO: ensure this overhead is correct, maybe pass it as an input of some kind through the config
        // We have the following overhead:
        //   time_series_id (integer) = 4 bytes
        //   start_time (long/bigint) = 8 bytes
        //   end_time (int) = 4 bytes
        //   value_timestamp_model_type (smallint) = 2 bytes
        //   bytea (varbyte) in postgresql has an overhead of 4 bytes, this goes for both the blobs = 4 + 4 bytes
        int overhead = 4 + 8 + 4 + 2 + 4 + 4;

        boolean populateSummaryTable = ConfigProperties.getInstance().populateSegmentSummary();
        if (populateSummaryTable) { //TODO if we join the summary information onto the segment table this needs adjustment
            //time_series_id = 4 bytes
            //start_time = 8 bytes
            //min_value = 4 byte
            //max_value = 4 byte
            // TODO: if we keep amount data points on the summary then add it here
            overhead += 4 + 8 + 4 + 4;
        }
        return overhead / 2; // there are two models to share the overhead
    }
}
