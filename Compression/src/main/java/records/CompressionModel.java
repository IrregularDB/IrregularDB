package records;

import compression.timestamp.TimestampCompressionModel;
import compression.value.ValueCompressionModel;

public record CompressionModel(ValueCompressionModel valueCompressionModel, TimestampCompressionModel timestampCompressionModel) {}