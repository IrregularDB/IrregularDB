package records.blob.data;

import compression.timestamp.TimeStampCompressionModelType;

import java.nio.ByteBuffer;

public record TimeStampBlobData(ByteBuffer timeStampBlobData, TimeStampCompressionModelType timeStampModelType) {
}
