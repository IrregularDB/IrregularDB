package records.blob.data;

import compression.timestamp.TimestampCompressionModelType;

import java.nio.ByteBuffer;

public record TimestampBlobData(ByteBuffer timeStampBlobData, TimestampCompressionModelType timestampModelType) {
}
