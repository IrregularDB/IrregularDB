package data.producer;

import config.ConfigProperties;
import sources.CSVTimeSeriesReader;

import java.io.File;
import java.io.IOException;
import java.util.Set;

public class SocketProducerSpawner {

    private final String csvDelimiter;
    private final Set<File> csvFiles;


    public SocketProducerSpawner(Set<File> csvFiles, String csvDelimiter) {
        this.csvFiles = csvFiles;
        this.csvDelimiter = csvDelimiter;
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
                SocketProducerCSVReader socketProducerCSVReader = new SocketProducerCSVReader(csvFile, csvDelimiter, serverIp, serverPort);
                socketProducerCSVReader.run();
                Thread.sleep(5);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
