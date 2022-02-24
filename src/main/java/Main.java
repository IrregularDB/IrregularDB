import config.ConfigProperties;
import scheduling.IncrementPartitioner;
import scheduling.Partitioner;
import scheduling.WorkingSetFactory;
import sources.CSVDataReceiverSpawner;
import sources.DataReceiverSpawner;

public class Main {

    public static void main(String[] args) {
        // TODO: Should be configurable which partitioner should be used
        Partitioner partitioner = new IncrementPartitioner(new WorkingSetFactory(), ConfigProperties.INSTANCE.getConfiguredNumberOfWorkingSets());

        initializeDataReceiverSpawners(partitioner);
    }

    private static void initializeDataReceiverSpawners(Partitioner partitioner) {
        DataReceiverSpawner dataReceiverSpawner = new CSVDataReceiverSpawner(partitioner, ConfigProperties.INSTANCE.getCsvSources());
        dataReceiverSpawner.spawn();
    }

}
