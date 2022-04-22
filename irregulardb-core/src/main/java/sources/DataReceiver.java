package sources;
import records.FinalizeTimeSeriesReading;
import records.TimeSeriesReading;
import scheduling.WorkingSet;
import utility.Stopwatch;

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

    /**
     * Similar documentation to that of WorkingSet.accept()
     */
    protected boolean sendTimeSeriesReadingToBuffer(TimeSeriesReading timeSeriesReading){
        int sizeBefore = timeSeriesTagsEmitted.size();
        timeSeriesTagsEmitted.add(timeSeriesReading.getTag());
        if (sizeBefore != timeSeriesTagsEmitted.size()) {
            // New tag is added
            Stopwatch.putStartTime(timeSeriesReading.getTag());
        }

        return this.workingSet.accept(timeSeriesReading);
    }

    public abstract void receiveData();

    public void close(){
        for (String tag : timeSeriesTagsEmitted) {
            this.workingSet.accept(new FinalizeTimeSeriesReading(tag));
        }
    }
}
