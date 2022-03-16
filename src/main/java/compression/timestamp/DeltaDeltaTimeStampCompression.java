package compression.timestamp;

import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class DeltaDeltaTimeStampCompression extends TimeStampCompressionModel{

    private List<Integer> deltaDeltaTimeStamps;
    private Long previousValue = null;
    private Integer previousDelta;

    public DeltaDeltaTimeStampCompression(Float errorBound, Integer lengthBound) {
        super(errorBound, lengthBound);
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
        return this.deltaDeltaTimeStamps.size();
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
        return null;
    }

    @Override
    protected void reduceToSize(int n) {
        if (n <= 0){
            throw new IllegalArgumentException("n cannot be 0 or lower");
        } else if (n > this.deltaDeltaTimeStamps.size()){
            throw new IllegalArgumentException("n cannot bigger than list size");
        }

        this.deltaDeltaTimeStamps.subList(n, deltaDeltaTimeStamps.size()).clear();
    }

    @Override
    public TimeStampCompressionModelType getTimeStampCompressionModelType() {
        return TimeStampCompressionModelType.DELTADELTA;
    }
}
