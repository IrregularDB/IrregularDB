package records.blob.data;

import compression.value.ValueCompressionModelType;

import java.nio.ByteBuffer;

public record ValueBlobData(ByteBuffer valueBlobData, ValueCompressionModelType valueModelType) {
}
