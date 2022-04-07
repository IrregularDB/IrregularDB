package records;

import compression.timestamp.TimestampCompressionModel;
import compression.timestamp.TimestampCompressionModelType;
import compression.value.ValueCompressionModel;
import compression.value.ValueCompressionModelType;

import java.nio.ByteBuffer;

public record CompressionModel(ValueCompressionModelType valueType, ByteBuffer valueCompressionModel, TimestampCompressionModelType timestampType, ByteBuffer timestampCompressionModel, int length) {}