package scheduling;

import config.ConfigProperties;
import records.TimeSeriesReading;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;

public class MapPartitioner extends Partitioner{

    private int nextWorkingSetIndex;

    public MapPartitioner(WorkingSetFactory workingSetFactory) {
        super(workingSetFactory);
        this.nextWorkingSetIndex = 0;
    }

    @Override
    public WorkingSet workingSetToSpawnReceiverFor() {
        WorkingSet workingSet = this.workingSets.get(nextWorkingSetIndex);
        this.nextWorkingSetIndex = ++this.nextWorkingSetIndex % numberOfWorkingSets;
        return workingSet;
    }
}
