package scheduling;

import records.TimeSeriesReading;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

public class MapPartitioner extends Partitioner{

    private final Map<String, WorkingSet> timeSeriesKeyToWorkingSet = new HashMap<>();
    private int nextWorkingSetIndex;

    public MapPartitioner(WorkingSetFactory workingSetFactory, Queue<TimeSeriesReading> threadSafeBuffer) {
        super(workingSetFactory, threadSafeBuffer);
        this.nextWorkingSetIndex = 0;
    }

    @Override
    public void performPartitioning(){
        while(true){
            TimeSeriesReading entry = threadSafeBuffer.poll();
            if (entry == null) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }

            if (!timeSeriesKeyToWorkingSet.containsKey(entry.tag())) {
                this.timeSeriesKeyToWorkingSet.put(entry.tag(), workingSets.get(nextWorkingSetIndex));
                this.nextWorkingSetIndex = ++this.nextWorkingSetIndex % numberOfWorkingSets;
            }

            this.timeSeriesKeyToWorkingSet.get(entry.tag()).accept(entry);
        }
    }
}
