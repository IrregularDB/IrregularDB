import config.ConfigProperties;
import scheduling.IncrementPartitioner;
import scheduling.Partitioner;
import scheduling.WorkingSetFactory;
import sources.CSVDataReceiverSpawner;
import sources.DataReceiverSpawner;
import storage.PostgresConnection;
import utility.Stopwatch;

import java.io.File;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

public class Main {

    public static void main(String[] args) throws SQLException {
        PostgresConnection.resetTables();

        // TODO: Should be configurable which partitioner should be used
        Partitioner partitioner = new IncrementPartitioner(new WorkingSetFactory(), ConfigProperties.getInstance().getConfiguredNumberOfWorkingSets());

        initializeDataReceiverSpawners(partitioner);
        Stopwatch.setInitialStartTime();
    }

    private static void initializeDataReceiverSpawners(Partitioner partitioner) {
        Set<File> csvSources = ConfigProperties.getInstance().getCsvSources();

        if (!csvSources.isEmpty()){
            DataReceiverSpawner dataReceiverSpawner = new CSVDataReceiverSpawner(partitioner, csvSources);
            dataReceiverSpawner.spawn();
        }
    }
}
