package compression.timestamp;

import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.LinkedList;

public class BaseDeltaTimeStampCompressionModel extends TimeStampCompressionModel {
    private long startTime;
    private LinkedList<Integer> deltaTimes;

    public BaseDeltaTimeStampCompressionModel(float errorBound) {
        super(errorBound, null);
        this.resetModel();
    }

    @Override
    protected void resetModel() {
        this.startTime = -1; // -1 -> startTime not yet set
        this.deltaTimes = new LinkedList<>();
    }

    @Override
    public int getLength() {
        return deltaTimes.size();
    }

    @Override
    protected boolean appendDataPoint(DataPoint dataPoint) {
        if (startTime == -1) {
            startTime = dataPoint.timestamp();
            return true;
        }
        this.deltaTimes.addLast(getDeltaFromStartTime(dataPoint.timestamp()));
        return true;
    }

    @Override
    protected ByteBuffer createByteBuffer() {
        ByteBuffer byteBuffer = ByteBuffer.allocate((this.deltaTimes.size()) * 4);
        for (Integer integer : this.deltaTimes) {
            byteBuffer.putInt(integer);
        }
        return byteBuffer;
    }

    @Override
    protected void reduceToSize(int n) {
        if (n <= 0){
            throw new IllegalArgumentException("n cannot be 0 or lower");
        } else if (n > this.deltaTimes.size()){
            throw new IllegalArgumentException("n cannot bigger than list size");
        }

        this.deltaTimes.subList(n, deltaTimes.size()).clear();
    }

    @Override
    public TimeStampCompressionModelType getTimeStampCompressionModelType() {
        return TimeStampCompressionModelType.BASEDELTA;
    }

    private int getDeltaFromStartTime(long timestamp) {
        long longDelta = timestamp - startTime;
        int delta = (int) (timestamp - startTime);
        if (delta != longDelta) {
            System.out.println("BaseDeltaTimeStampCompressionModel:getDeltaFromStartTime(): \"Timestamp not " +
                    "represented correctly by delta \" ");
        }
        return delta;
    }

    public long getStartTime() {
        return startTime;
    }
}
