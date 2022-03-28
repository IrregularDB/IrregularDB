package scheduling;

import segmentgenerator.TestTimeSeriesFactory;
import storage.TestDatabaseConnectionFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

public class TestWorkingSetFactory extends WorkingSetFactory {

    @Override
    public WorkingSet generateWorkingSet(){
        return new TestWorkingSet(new ConcurrentLinkedQueue<>(), new TestTimeSeriesFactory(), new TestDatabaseConnectionFactory());
    }
}
