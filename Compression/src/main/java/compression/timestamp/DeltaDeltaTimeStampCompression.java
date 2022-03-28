package compression.timestamp;

import compression.encoding.SignedBucketEncoder;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DeltaDeltaTimeStampCompression extends TimeStampCompressionModel{
    private final SignedBucketEncoder signedBucketEncoder;
    private List<Integer> deltaDeltaTimeStamps;
    private Long previousValue = null;
    private Integer previousDelta;

    public DeltaDeltaTimeStampCompression() {
        super(null, null);
        // We make this a field so that we don't have to allocate a new signed bucket encoder each time get byte buffer is called
        signedBucketEncoder = new SignedBucketEncoder();
        resetModel();
    }

    @Override
    protected void resetModel() {
        this.deltaDeltaTimeStamps = new ArrayList<>();
        this.previousValue = null;
        this.previousDelta = null;
    }

    @Override
    public int getLength() {
        // The first time stamp is not included in the list
        return 1 + this.deltaDeltaTimeStamps.size();
    }

    @Override
    protected boolean appendDataPoint(DataPoint dataPoint) {
        if (this.deltaDeltaTimeStamps.size() == 0 && previousValue == null){
            // Don't store anything for first timestamp but remember it for next time
            previousValue = dataPoint.timestamp();
        } else if (this.deltaDeltaTimeStamps.size() == 0){
            // Save the first entry as the delta value.
            Integer delta = (int) (dataPoint.timestamp() - previousValue);
            deltaDeltaTimeStamps.add(delta);
            previousValue = dataPoint.timestamp();
            previousDelta = delta;
        } else {
            // Save the remaining entries as deltadelta
            Integer delta = (int) (dataPoint.timestamp() - previousValue);
            Integer deltaDelta = delta - previousDelta;
            deltaDeltaTimeStamps.add(deltaDelta);
            previousValue = dataPoint.timestamp();
            previousDelta = delta;
        }
        return true;
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        return signedBucketEncoder.encode(this.deltaDeltaTimeStamps).getFinishedByteBuffer();
    }

    @Override
    protected void reduceToSize(int n) {
        // We have to cut the list down to size n-1 as the first time stamp is not in the list
        this.deltaDeltaTimeStamps.subList(n - 1, deltaDeltaTimeStamps.size()).clear();
    }

    @Override
    public TimeStampCompressionModelType getTimeStampCompressionModelType() {
        return TimeStampCompressionModelType.DELTADELTA;
    }
}
