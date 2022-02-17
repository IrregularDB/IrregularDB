package scheduling;

import records.TimeSeriesReading;

import java.util.*;

public abstract class Partitioner {

    protected List<WorkingSet> workingSets = new ArrayList<>();
    protected final int numberOfWorkingSets;
    private final WorkingSetFactory workingSetFactory;

    public Partitioner(WorkingSetFactory workingSetFactory, int numberOfWorkingSets){
        this.workingSetFactory = workingSetFactory;

        this.numberOfWorkingSets = numberOfWorkingSets;

        initializeWorkingSets();
    }

    private void initializeWorkingSets(){
        for (int i = 0; i < numberOfWorkingSets; i++) {
            WorkingSet workingSet = workingSetFactory.generateWorkingSet();
            workingSets.add(workingSet);
            new Thread(workingSet::run).start();
        }
    }

    public abstract WorkingSet workingSetToSpawnReceiverFor();
}
