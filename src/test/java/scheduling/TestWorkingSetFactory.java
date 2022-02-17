package scheduling;

import records.TimeSeriesReading;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TestWorkingSetFactory extends WorkingSetFactory{

    private final Queue<TestWorkingSet> generatedWorkingSets = new ConcurrentLinkedQueue<>();

    @Override
    public WorkingSet generateWorkingSet() {
        TestWorkingSet testWorkingSet = new TestWorkingSet();
        this.generatedWorkingSets.add(testWorkingSet);
        return testWorkingSet;
    }

    public Queue<TestWorkingSet> getGeneratedWorkingSets() {
        return generatedWorkingSets;
    }

}

