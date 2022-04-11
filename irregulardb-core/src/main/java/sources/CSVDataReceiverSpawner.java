package sources;

import scheduling.Partitioner;
import scheduling.WorkingSet;

import java.io.File;
import java.util.Set;

public class CSVDataReceiverSpawner extends DataReceiverSpawner{

    private static final String CSV_DELIMITER = ",";

    private final Set<File> csvFiles;

    
    public CSVDataReceiverSpawner(Partitioner partitioner, Set<File> csvFiles) {
        super(partitioner);
        this.csvFiles = csvFiles;
    }
    
    @Override
    public void spawn() {
        for (File csvPath : this.csvFiles) {
            WorkingSet workingSet = super.partitioner.workingSetToSpawnReceiverFor();

            CSVDataReceiver csvDataReceiver = new CSVDataReceiver(csvPath, workingSet, CSV_DELIMITER);
            runReceiverInThread(csvDataReceiver);
        }
    }
}
