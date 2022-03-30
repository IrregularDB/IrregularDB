package sources;
import records.TimeSeriesReading;
import scheduling.WorkingSet;

public abstract class DataReceiver {

    private final WorkingSet workingSet;

    public DataReceiver(WorkingSet workingSet) {
        this.workingSet = workingSet;
    }

    protected void sendTimeSeriesReadingToBuffer(TimeSeriesReading timeSeriesReading){
        this.workingSet.accept(timeSeriesReading);
    }

    public abstract void receiveData();

    public void close(){
        for (String tag : timeSeriesTagsEmitted) {
            this.workingSet.accept(new FinalizeTimeSeriesReading(tag, null));
        }
    }
}
