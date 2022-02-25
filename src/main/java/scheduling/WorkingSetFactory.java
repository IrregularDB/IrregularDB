package scheduling;

import segmentgenerator.TimeSeriesFactory;
import storage.DatabaseConnectionFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public class WorkingSetFactory {

    public WorkingSet generateWorkingSet(){
        return new WorkingSet(new ConcurrentLinkedQueue<>(), new TimeSeriesFactory());
    }

}
