package records;

import java.nio.ByteBuffer;

// TODO: FOR REGULÆRE TIDSSERIER: GEMMER VI ENDTIME I BLOBDATA FOR TIMESTAMP SÅ VI IKKE SKAL BRUGE ENDTIME????
public record Segment(String timeSeriesKey, long startTime, long endTime, int valueModelType, ByteBuffer valueBlob, int timestampModelType, ByteBuffer timestampBlob) {
}
