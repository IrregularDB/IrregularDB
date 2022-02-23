package compression.timestamp;

import records.DataPoint;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

public class BaseDeltaTimeStampCompressionModel extends TimeStampCompressionModel{

    private long startTime;
    private LinkedList<Integer> deltaTimeStamps;

    public BaseDeltaTimeStampCompressionModel(double errorBound) {
        super(errorBound);
        this.resetModel();
    }

    @Override
    protected void resetModel() {
        this.startTime = -1; // -1 -> startTime not yet set
        this.deltaTimeStamps = new LinkedList<>();
    }

    @Override
    public int getLength() {
        return deltaTimeStamps.size();
    }

    @Override
    public boolean append(DataPoint dataPoint) {
        if (startTime < 0){
            startTime = dataPoint.timestamp();
            return true;
        }

        this.deltaTimeStamps.addLast(getDeltaFromStartTime(dataPoint.timestamp()));

        return true;
    }

    @Override
    public ByteBuffer getBlobRepresentation() {
        ByteBuffer byteBuffer = ByteBuffer.allocate(this.deltaTimeStamps.size() * 4);
        for (Integer integer : this.deltaTimeStamps){
            byteBuffer.putInt(integer);
        }
        return byteBuffer;
    }

    @Override
    public void reduceToSizeN(int n) {
        for(int i = 0; i < n; n++){
            this.deltaTimeStamps.removeLast();
        }
    }

    @Override
    public TimeStampCompressionModelType getTimeStampCompressionModelType() {
        return TimeStampCompressionModelType.BASEDELTA;
    }

    private int getDeltaFromStartTime(long timestamp){
        int delta = (int) (timestamp - startTime);
        if (delta != timestamp){
            System.out.println("BaseDeltaTimeStampCompressionModel:getDeltaFromStartTime(): \"Timestamp not " +
                    "represented correctly by delta \" ");
        }
        return delta;
    }
}
