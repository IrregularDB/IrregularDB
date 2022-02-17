package scheduling;

public class IncrementPartitioner extends Partitioner{

    private int nextWorkingSetIndex;

    public IncrementPartitioner(WorkingSetFactory workingSetFactory) {
        super(workingSetFactory);
        this.nextWorkingSetIndex = 0;
    }

    @Override
    public WorkingSet workingSetToSpawnReceiverFor() {
        WorkingSet workingSet = super.workingSets.get(nextWorkingSetIndex);
        this.nextWorkingSetIndex = (this.nextWorkingSetIndex + 1) % numberOfWorkingSets;
        return workingSet;
    }
}
