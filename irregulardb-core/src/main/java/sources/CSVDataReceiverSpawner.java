package sources;

import scheduling.Partitioner;
import scheduling.WorkingSet;

import java.io.File;
import java.util.Set;

public class CSVDataReceiverSpawner extends DataReceiverSpawner{

    private final String csvDelimiter;
    private final Set<File> csvFiles;
    
    public CSVDataReceiverSpawner(Partitioner partitioner, Set<File> csvFiles, String csvDelimiter) {
        super(partitioner);
        this.csvFiles = csvFiles;
        this.csvDelimiter = csvDelimiter;
    }
    
    @Override
    public void spawn() {
        new Thread(this::startCSVReceivers).start();
    }

    private void startCSVReceivers() {
        for (File csvFile : this.csvFiles) {
            if (!csvFile.exists()) {
                System.out.println("Could not find the file with the path: " + csvFile.getAbsolutePath());
                continue;
            }

            WorkingSet workingSet = super.partitioner.workingSetToSpawnReceiverFor();
            CSVDataReceiver csvDataReceiver = new CSVDataReceiver(csvFile, workingSet, csvDelimiter);
            runReceiverInThread(csvDataReceiver);
        }
    }
}
