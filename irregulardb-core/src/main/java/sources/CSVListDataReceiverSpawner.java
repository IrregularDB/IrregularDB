package sources;

import config.ConfigProperties;
import records.Pair;
import scheduling.Partitioner;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class CSVListDataReceiverSpawner extends DataReceiverSpawner {
    private static final int AMT_WORKING_SETS = ConfigProperties.getInstance().getConfiguredNumberOfWorkingSets();
    private final List<Pair<File, String>> fileAndTag;
    private final String csvDelimiter;

    public CSVListDataReceiverSpawner(Partitioner partitioner, List<Pair<File, String>> fileAndTag, String csvDelimiter) {
        super(partitioner);
        this.fileAndTag = fileAndTag;
        this.csvDelimiter = csvDelimiter;
    }

    @Override
    public void spawn() {
        int amtFilesPerListReceiver = fileAndTag.size() / AMT_WORKING_SETS;
        while (!fileAndTag.isEmpty()) {
            List<Pair<File, String>> pairs;
            if (fileAndTag.size() > amtFilesPerListReceiver) {
                pairs = fileAndTag.subList(0, amtFilesPerListReceiver);
            } else {
                pairs = fileAndTag.subList(0, fileAndTag.size());
            }
            ArrayList<Pair<File, String>> pairsForDataReceiver = new ArrayList<>(pairs);
            pairs.clear();

            CSVListDataReceiver csvListDataReceiver = new CSVListDataReceiver(pairsForDataReceiver, super.partitioner.workingSetToSpawnReceiverFor(), csvDelimiter);

            runReceiverInThread(csvListDataReceiver);

        }


    }
}
