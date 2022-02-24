package compression.timestamp;

import compression.encoding.BucketEncoding;
import compression.utility.BitBuffer;
import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DeltaPairsTimeStampCompressionModel extends TimeStampCompressionModel {

    private Long prevTimeStamp;
    List<Integer> deltaTimeStamps;

    public DeltaPairsTimeStampCompressionModel(Float errorBound) {
        super(errorBound, -1);
        resetModel();
    }

    @Override
    protected void resetModel() {
        this.deltaTimeStamps = new ArrayList<>();
        this.prevTimeStamp = null;
    }

    @Override
    public int getLength() {
        return this.deltaTimeStamps.size();
    }

    @Override
    public boolean append(DataPoint dataPoint) {
        if (prevTimeStamp != null) {
            this.deltaTimeStamps.add((int) (dataPoint.timestamp() - prevTimeStamp));
        }
        prevTimeStamp = dataPoint.timestamp();
        return true;
    }

    @Override
    public ByteBuffer getBlobRepresentation() {
        BitBuffer encode = BucketEncoding.encode(this.deltaTimeStamps);
        return encode.getByteBuffer();
    }

    @Override
    public void reduceToSizeN(int n) {
        if (n <= 0){
            throw new IllegalArgumentException("n cannot be 0 or lower");
        } else if (n > this.deltaTimeStamps.size()){
            throw new IllegalArgumentException("n cannot bigger than list size");
        }

        this.deltaTimeStamps.subList(n, deltaTimeStamps.size()).clear();
    }

    @Override
    public TimeStampCompressionModelType getTimeStampCompressionModelType() {
        return TimeStampCompressionModelType.DELTAPAIRS;
    }
}
