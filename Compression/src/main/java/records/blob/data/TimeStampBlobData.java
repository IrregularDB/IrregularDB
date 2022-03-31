package records.blob.data;

import compression.timestamp.TimestampCompressionModelType;

import java.nio.ByteBuffer;

public record TimeStampBlobData(ByteBuffer timeStampBlobData, TimestampCompressionModelType timeStampModelType) {
}
