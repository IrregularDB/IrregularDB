import config.ConfigProperties;
import data.producer.SocketProducerSpawner;
import scheduling.IncrementPartitioner;
import scheduling.Partitioner;
import scheduling.WorkingSetFactory;
import sources.CSVDataReceiverSpawner;
import sources.DataReceiverSpawner;
import sources.SocketDataReceiverSpawner;
import storage.PostgresConnection;
import utility.Stopwatch;

import java.io.File;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

public class Main {

    public static void main(String[] args) throws SQLException, InterruptedException {
        PostgresConnection.resetTables();

        // TODO: Should be configurable which partitioner should be used
        Partitioner partitioner = new IncrementPartitioner(new WorkingSetFactory(), ConfigProperties.getInstance().getConfiguredNumberOfWorkingSets());

        //initializeCSVDataReceiverSpawner(partitioner);
        initializeSocketData(partitioner);
        Stopwatch.setInitialStartTime();
    }

    private static void initializeCSVDataReceiverSpawner(Partitioner partitioner) {
        Set<File> csvSources = ConfigProperties.getInstance().getCsvSources();

        if (!csvSources.isEmpty()){
            String csvDelimiter = ConfigProperties.getInstance().getCsvDelimiter();
            DataReceiverSpawner dataReceiverSpawner = new CSVDataReceiverSpawner(partitioner, csvSources, csvDelimiter);
            dataReceiverSpawner.spawn();
        }
    }

    private static void initializeSocketData(Partitioner partitioner) throws InterruptedException {
        initializeSocketReceiverSpawner(partitioner);
        Thread.sleep(100);
        initializeSocketProducerSpawner();
    }

    private static void initializeSocketReceiverSpawner(Partitioner partitioner) {
        int socketPort = ConfigProperties.getInstance().getSocketDataReceiverSpawnerPort();

        DataReceiverSpawner dataReceiverSpawner = new SocketDataReceiverSpawner(partitioner, socketPort);
        dataReceiverSpawner.spawn();
    }

    private static void initializeSocketProducerSpawner() {
        Set<File> csvSources = ConfigProperties.getInstance().getCsvSources();

        if (!csvSources.isEmpty()){
            String csvDelimiter = ConfigProperties.getInstance().getCsvDelimiter();
            SocketProducerSpawner spawner = new SocketProducerSpawner(csvSources, csvDelimiter);
            spawner.spawn();
        }
    }
}
