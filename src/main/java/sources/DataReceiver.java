package sources;
import records.TimeSeriesReading;

import java.io.FileNotFoundException;
import java.util.Queue;

public abstract class DataReceiver {

    private Queue<TimeSeriesReading> systemTimeSeriesBuffer;

    public DataReceiver(Queue<TimeSeriesReading> systemTimeSeriesBuffer) {
        this.systemTimeSeriesBuffer = systemTimeSeriesBuffer;
    }

    public void sendTimeSeriesReadingToBuffer(TimeSeriesReading timeSeriesReading){
        this.systemTimeSeriesBuffer.add(timeSeriesReading);
    }

    public abstract void run() throws FileNotFoundException;
}
