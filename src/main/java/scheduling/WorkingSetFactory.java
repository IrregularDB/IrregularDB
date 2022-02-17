package scheduling;

import records.TimeSeriesReading;
import segmentgenerator.TimeSeriesFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public class WorkingSetFactory {

    public WorkingSet generateWorkingSet(){
        return new WorkingSet(new ConcurrentLinkedQueue<>());
    }

}
