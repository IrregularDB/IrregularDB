package data.producer;

import config.ConfigProperties;
import sources.CSVTimeSeriesReader;

import java.io.File;
import java.io.IOException;
import java.util.Set;

public class SocketProducerSpawner {

    private static final String CSV_DELIMITER = " ";
    private final Set<File> csvFiles;


    public SocketProducerSpawner(Set<File> csvFiles) {
        this.csvFiles = csvFiles;
    }

    public void spawn() {
        new Thread(this::startCSVSocketProducers).start();
    }

    private void startCSVSocketProducers() {
        int serverPort = ConfigProperties.getInstance().getSocketDataReceiverSpawnerPort();
        String serverIp = "localhost";

        for (File csvFile : this.csvFiles) {
            if (!csvFile.exists()) {
                System.out.println("Could not find the file with the path: " + csvFile.getAbsolutePath());
                continue;
            }

            try {
                CSVTimeSeriesReader csvTimeSeriesReader = new CSVTimeSeriesReader(csvFile, CSV_DELIMITER);
                SocketProducerCSVReader socketProducerCSVReader = new SocketProducerCSVReader(csvTimeSeriesReader, serverIp, serverPort);
                socketProducerCSVReader.run();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

}
