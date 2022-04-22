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
        new Thread(this::startCSVReceivers).start();
    }

    private void startCSVReceivers() {
        for (File csvFile : this.csvFiles) {
            if (!csvFile.exists()) {
                System.out.println("Could not find the file with the path: " + csvFile.getAbsolutePath());
                continue;
            }

            WorkingSet workingSet = super.partitioner.workingSetToSpawnReceiverFor();
            while (workingSet.getAmtActiveTimeSeries() > MAX_ACTIVE_RECEIVERS_FOR_CSV){
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                workingSet = super.partitioner.workingSetToSpawnReceiverFor();
            }

            CSVDataReceiver csvDataReceiver = new CSVDataReceiver(csvFile, workingSet, CSV_DELIMITER);
            runReceiverInThread(csvDataReceiver);
        }
    }
}
