import scheduling.MapPartitioner;
import scheduling.Partitioner;
import scheduling.WorkingSetFactory;
import sources.CSVDataReceiverSpawner;
import sources.DataReceiverSpawner;


public class Main {

    public static void main(String[] args) {
        // TODO: Should be configurable which partitioner should be used
        Partitioner partitioner = new MapPartitioner(new WorkingSetFactory(), 2);

        initializeDataReceiverSpawners(partitioner);
    }

    private static void initializeDataReceiverSpawners(Partitioner partitioner) {
        DataReceiverSpawner dataReceiverSpawner = new CSVDataReceiverSpawner(partitioner, "src/main/resources/test.csv" );
        dataReceiverSpawner.spawn();
    }
}
