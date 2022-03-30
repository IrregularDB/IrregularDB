package sources;
import records.FinalizeTimeSeriesReading;
import records.TimeSeriesReading;
import scheduling.WorkingSet;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class DataReceiver {

    private final WorkingSet workingSet;
    private final Set<String> timeSeriesTagsEmitted;

    public DataReceiver(WorkingSet workingSet) {
        this.workingSet = workingSet;
        this.timeSeriesTagsEmitted = new HashSet<>();
    }

    protected void sendTimeSeriesReadingToBuffer(TimeSeriesReading timeSeriesReading){
        timeSeriesTagsEmitted.add(timeSeriesReading.getTag());
        this.workingSet.accept(timeSeriesReading);
    }

    public abstract void receiveData();

    public void close(){
        for (String tag : timeSeriesTagsEmitted) {
            this.workingSet.accept(new FinalizeTimeSeriesReading(tag, null));
        }
    }
}
