package sources;
import records.FinalizeTimeSeriesReading;
import records.TimeSeriesReading;
import scheduling.WorkingSet;
import utility.Stopwatch;

import java.util.HashSet;
import java.util.Set;

public abstract class DataReceiver {

    private final WorkingSet workingSet;
    private final Set<String> timeSeriesTagsEmitted;
    private String lastTagReceived;

    public DataReceiver(WorkingSet workingSet) {
        this.workingSet = workingSet;
        this.timeSeriesTagsEmitted = new HashSet<>();
        this.lastTagReceived ="";
    }

    /**
     * Similar documentation to that of WorkingSet.accept()
     */
    protected boolean sendTimeSeriesReadingToBuffer(TimeSeriesReading timeSeriesReading){
        String tag = timeSeriesReading.getTag();
        // Speedup trying to reduce the amount of calls to contains()
        if (!tag.equals(this.lastTagReceived)) {
            if (!this.timeSeriesTagsEmitted.contains(tag)) {
                timeSeriesTagsEmitted.add(tag);
                Stopwatch.putStartTime(timeSeriesReading.getTag());
            }
            this.lastTagReceived = tag;
        }

        return this.workingSet.accept(timeSeriesReading);
    }

    public abstract void receiveData();

    public void close(){
        for (String tag : timeSeriesTagsEmitted) {
            this.workingSet.accept(new FinalizeTimeSeriesReading(tag));
        }
        this.timeSeriesTagsEmitted.clear();
    }
}
