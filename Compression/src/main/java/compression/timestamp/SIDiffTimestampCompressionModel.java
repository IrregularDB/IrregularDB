package compression.timestamp;

import compression.encoding.SignedBucketEncoder;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class SIDiffTimestampCompressionModel extends TimestampCompressionModel {
    private final SignedBucketEncoder signedBucketEncoder;
    private Long firstTimeStamp;
    private List<Long> timeStamps;


    public SIDiffTimestampCompressionModel() {
        super(null);
        signedBucketEncoder = new SignedBucketEncoder();
        this.resetModel();
    }

    @Override
    protected void resetModel() {
        this.timeStamps = new ArrayList<>();
        this.firstTimeStamp = null;
    }

    @Override
    public int getLength() {
        return timeStamps.size();
    }

    @Override
    protected boolean appendDataPoint(DataPoint dataPoint) {
        return false;
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        return null;
    }

    @Override
    protected void reduceToSize(int n) {

    }

    @Override
    public TimestampCompressionModelType getTimeStampCompressionModelType() {
        return TimestampCompressionModelType.SIDIFF;
    }
}
