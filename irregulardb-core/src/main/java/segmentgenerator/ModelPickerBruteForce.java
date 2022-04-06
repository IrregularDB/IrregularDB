package segmentgenerator;

import compression.BaseModel;
import compression.timestamp.TimestampCompressionModel;
import compression.value.ValueCompressionModel;
import records.CompressionModel;

import java.util.List;

public class ModelPickerBruteForce extends ModelPicker{
    @Override
    public CompressionModel findBestCompressionModel(List<ValueCompressionModel> valueCompressionModels, List<TimestampCompressionModel> timestampCompressionModels) {
        return null;
    }

    @Override
    protected double calculateAmountBytesPerDataPoint(BaseModel model) {
        return super.calculateAmountBytesPerDataPoint(model);
    }
}
