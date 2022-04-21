package sources;

import scheduling.Partitioner;
import scheduling.WorkingSet;

import java.io.File;
import java.util.Set;

import static scheduling.WorkingSet.MAX_ACTIVE_RECEIVERS_FOR_CSV;

public class CSVDataReceiverSpawner extends DataReceiverSpawner{

    private static final String CSV_DELIMITER = " ";
    private final Set<File> csvFiles;
    
    public CSVDataReceiverSpawner(Partitioner partitioner, Set<File> csvFiles) {
        super(partitioner);
        this.csvFiles = csvFiles;
    }
    
    @Override
    public void spawn() {
        for (File csvPath : this.csvFiles) {
            WorkingSet workingSet = super.partitioner.workingSetToSpawnReceiverFor();
            while (workingSet.getActiveTimeSeries() > MAX_ACTIVE_RECEIVERS_FOR_CSV){
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                workingSet = super.partitioner.workingSetToSpawnReceiverFor();
            }

            CSVDataReceiver csvDataReceiver = new CSVDataReceiver(csvPath, workingSet, CSV_DELIMITER);
            runReceiverInThread(csvDataReceiver);
        }
    }
}
