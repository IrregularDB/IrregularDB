package scheduling;

import config.ConfigProperties;
import records.TimeSeriesReading;

import java.util.*;

public abstract class Partitioner {

    protected List<WorkingSet> workingSets = new ArrayList<>();
    protected final int numberOfWorkingSets;
    private final WorkingSetFactory workingSetFactory;

    public Partitioner(WorkingSetFactory workingSetFactory){
        this.workingSetFactory = workingSetFactory;
        this.numberOfWorkingSets = ConfigProperties.INSTANCE.getConfiguredNumberOfWorkingSets();

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
