package sources;
import records.TimeSeriesReading;
import scheduling.WorkingSet;

import java.io.FileNotFoundException;
import java.util.Queue;

public abstract class DataReceiver {

    private WorkingSet workingSet;

    public DataReceiver(WorkingSet workingSet) {
        this.workingSet = workingSet;
    }

    public void sendTimeSeriesReadingToBuffer(TimeSeriesReading timeSeriesReading){
        this.workingSet.accept(timeSeriesReading);
    }

    public abstract void receiveData() throws FileNotFoundException;
}
