package records;

import java.nio.ByteBuffer;
import java.util.List;

// TODO: FOR REGULÆRE TIDSSERIER: GEMMER VI ENDTIME I BLOBDATA FOR TIMESTAMP SÅ VI IKKE SKAL BRUGE ENDTIME????
public record Segment(SegmentKey segmentKey, long endTime, byte valueModelType, ByteBuffer valueBlob, byte timestampModelType, ByteBuffer timestampBlob, SegmentSummary segmentSummary) {};