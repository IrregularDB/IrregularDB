package compression.timestamp;

import compression.utility.BitBuffer.BitBuffer;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DeltaPairsTimeStampCompressionModel extends TimeStampCompressionModel {

    private Long prevTimeStamp;
    private List<Integer> deltaTimes;

    public DeltaPairsTimeStampCompressionModel() {
        super(null, null);
        resetModel();
    }

    @Override
    protected void resetModel() {
        this.deltaTimes = new ArrayList<>();
        this.prevTimeStamp = null;
    }

    @Override
    public int getLength() {
        return this.deltaTimes.size();
    }

    @Override
    protected boolean appendDataPoint(DataPoint dataPoint) {
        if (prevTimeStamp != null) {
            this.deltaTimes.add((int) (dataPoint.timestamp() - prevTimeStamp));
        }
        prevTimeStamp = dataPoint.timestamp();
        return true;
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        BitBuffer encode = bucketEncoding.encode(this.deltaTimes);
        return encode.getFinishedByteBuffer();
    }

    @Override
    protected void reduceToSize(int n) {
        this.deltaTimes.subList(n, deltaTimes.size()).clear();
    }

    @Override
    public TimeStampCompressionModelType getTimeStampCompressionModelType() {
        return TimeStampCompressionModelType.DELTAPAIRS;
    }
}
