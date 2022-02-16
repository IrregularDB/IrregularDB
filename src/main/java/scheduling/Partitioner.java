package scheduling;

import records.TimeSeriesReading;

import java.util.*;

public abstract class Partitioner {
    protected List<WorkingSet> workingSets = new ArrayList<>();
    protected final int numberOfWorkingSets;
    private final WorkingSetFactory workingSetFactory;
    protected final Queue<TimeSeriesReading> threadSafeBuffer;

    public Partitioner(WorkingSetFactory workingSetFactory, Queue<TimeSeriesReading> threadSafeBuffer){
        this.workingSetFactory = workingSetFactory;
        this.threadSafeBuffer = threadSafeBuffer;

        numberOfWorkingSets = 6;

        initializeWorkingSets();
    }

    private void initializeWorkingSets(){
        for (int i = 0; i < numberOfWorkingSets; i++) {
            WorkingSet workingSet = workingSetFactory.generateWorkingSet();
            workingSets.add(workingSet);
            new Thread(workingSet::run).start();
        }
    }

    public abstract void performPartitioning();

}
