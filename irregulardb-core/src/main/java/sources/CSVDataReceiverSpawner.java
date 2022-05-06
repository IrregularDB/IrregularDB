package sources;

import records.Pair;
import scheduling.Partitioner;
import scheduling.WorkingSet;

import java.io.File;
import java.util.List;

public class CSVDataReceiverSpawner extends DataReceiverSpawner{

    private final String csvDelimiter;
    private final List<Pair<File,String>> csvFilesWithFileTag;
    
    public CSVDataReceiverSpawner(Partitioner partitioner, List<Pair<File,String>> csvFilesWithFileTag, String csvDelimiter) {
        super(partitioner);
        this.csvFilesWithFileTag = csvFilesWithFileTag;
        this.csvDelimiter = csvDelimiter;
    }
    
    @Override
    public void spawn() {
        new Thread(this::startCSVReceivers).start();
    }

    private void startCSVReceivers() {
        for (Pair<File,String> csvFileWithFileTag : this.csvFilesWithFileTag) {
            if (!csvFileWithFileTag.f0().exists()) {
                System.out.println("Could not find the file with the path: " + csvFileWithFileTag.f0().getAbsolutePath());
                continue;
            }

            WorkingSet workingSet = super.partitioner.workingSetToSpawnReceiverFor();
            CSVDataReceiver csvDataReceiver = new CSVDataReceiver(csvFileWithFileTag.f0(), csvFileWithFileTag.f1(), workingSet, csvDelimiter);
            runReceiverInThread(csvDataReceiver);
        }
    }
}
