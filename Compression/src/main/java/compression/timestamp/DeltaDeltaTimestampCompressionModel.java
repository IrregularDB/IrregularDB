package compression.timestamp;

import compression.encoding.SignedBucketEncoder;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DeltaDeltaTimestampCompressionModel extends TimestampCompressionModel {
    private final SignedBucketEncoder signedBucketEncoder;
    private List<Integer> deltaDeltaTimestamps;
    private Long previousValue = null;
    private Integer previousDelta;

    public DeltaDeltaTimestampCompressionModel(Integer threshold) {
        super(threshold);
        // We make this a field so that we don't have to allocate a new signed bucket encoder each time get byte buffer is called
        signedBucketEncoder = new SignedBucketEncoder();
        resetModel();
    }

    @Override
    protected void resetModel() {
        this.deltaDeltaTimestamps = new ArrayList<>();
        this.previousValue = null;
        this.previousDelta = null;
    }

    @Override
    public int getLength() {
        // The first time stamp is not included in the list
        return 1 + this.deltaDeltaTimestamps.size();
    }

    @Override
    protected boolean appendDataPoint(DataPoint dataPoint) {
        if (this.deltaDeltaTimestamps.size() == 0 && previousValue == null){
            // Don't store anything for first timestamp but remember it for next time
            previousValue = dataPoint.timestamp();
        } else {
            Integer timestampToBeAdded = tryApplyThreshold(dataPoint);

            if (this.deltaDeltaTimestamps.size() == 0) {
                // Add the first value as delta
                previousValue = previousValue + timestampToBeAdded;
                previousDelta = timestampToBeAdded;
            } else {
                // Save the remaining entries as deltadelta
                previousDelta = previousDelta + timestampToBeAdded;
                previousValue = previousValue + previousDelta;
            }

            deltaDeltaTimestamps.add(timestampToBeAdded);
        }
        return true;
    }

    private Integer tryApplyThreshold(DataPoint dataPoint) {

        int delta = (int) (dataPoint.timestamp() - previousValue);
        int result;

        if (this.deltaDeltaTimestamps.size() == 0) {
            result = tryApplyThreshold(delta);
        } else {
            result = tryApplyThreshold(delta - previousDelta);
        }

        return result;
    }

    private Integer tryApplyThreshold(int value) {
        int absoluteValue = Math.abs(value);
        for (Integer maxValue : maxBucketValues) {
            if (maxValue < absoluteValue && (absoluteValue - getThreshold()) <= maxValue) {
               return value < 0 ? -maxValue : maxValue;
            }
        }
        return value;
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        return signedBucketEncoder.encode(this.deltaDeltaTimestamps).getFinishedByteBuffer();
    }

    @Override
    protected void reduceToSize(int n) {
        // We have to cut the list down to size n-1 as the first time stamp is not in the list
        this.deltaDeltaTimestamps.subList(n - 1, deltaDeltaTimestamps.size()).clear();
    }

    @Override
    public TimestampCompressionModelType getTimestampCompressionModelType() {
        return TimestampCompressionModelType.DELTADELTA;
    }
}
