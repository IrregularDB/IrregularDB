import config.ConfigProperties;
import scheduling.IncrementPartitioner;
import scheduling.Partitioner;
import scheduling.WorkingSetFactory;
import sources.CSVDataReceiverSpawner;
import sources.DataReceiverSpawner;

import java.util.List;

public class Main {

    public static void main(String[] args) {
        // TODO: Should be configurable which partitioner should be used
        Partitioner partitioner = new IncrementPartitioner(new WorkingSetFactory(), ConfigProperties.getInstance().getConfiguredNumberOfWorkingSets());

        initializeDataReceiverSpawners(partitioner);
    }

    private static void initializeDataReceiverSpawners(Partitioner partitioner) {
        List<String> csvSources = ConfigProperties.getInstance().getCsvSources();

        if (!csvSources.isEmpty()){
            DataReceiverSpawner dataReceiverSpawner = new CSVDataReceiverSpawner(partitioner, csvSources);
            dataReceiverSpawner.spawn();
        }
    }
}
