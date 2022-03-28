package sources;

import scheduling.Partitioner;
import scheduling.WorkingSet;

import java.util.Collections;
import java.util.List;

public class CSVDataReceiverSpawner extends DataReceiverSpawner{

    private static final String CSV_DELIMITER = ",";

    private final List<String> csvPaths;

    
    public CSVDataReceiverSpawner(Partitioner partitioner, List<String> csvPaths) {
        super(partitioner);
        this.csvPaths = csvPaths;
    }

    public CSVDataReceiverSpawner(Partitioner partitioner, String csvPath) {
        this(partitioner, Collections.singletonList(csvPath));
    }
    
    @Override
    public void spawn() {
        for (String csvPath : this.csvPaths) {
            WorkingSet workingSet = super.partitioner.workingSetToSpawnReceiverFor();

            CSVDataReceiver csvDataReceiver = new CSVDataReceiver(csvPath, workingSet, CSV_DELIMITER);
            runReceiverInThread(csvDataReceiver);
        }
    }
}
