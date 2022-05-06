package data.producer;

import config.ConfigProperties;
import records.Pair;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class SocketProducerSpawner {

    private final String csvDelimiter;
    private final List<Pair<File,String>> csvFilesWithFileTags;


    public SocketProducerSpawner(List<Pair<File,String>> csvFilesWithFileTags, String csvDelimiter) {
        this.csvFilesWithFileTags = csvFilesWithFileTags;
        this.csvDelimiter = csvDelimiter;
    }

    public void spawn() {
        new Thread(this::startCSVSocketProducers).start();
    }

    private void startCSVSocketProducers() {
        int serverPort = ConfigProperties.getInstance().getSocketDataReceiverSpawnerPort();
        String serverIp = "localhost";

        for (Pair<File,String> csvFileWithTag : this.csvFilesWithFileTags) {
            if (!csvFileWithTag.f0().exists()) {
                System.out.println("Could not find the file with the path: " + csvFileWithTag.f0().getAbsolutePath());
                continue;
            }

            try {
                SocketProducerCSVReader socketProducerCSVReader = new SocketProducerCSVReader(csvFileWithTag.f0(),csvFileWithTag.f1(), csvDelimiter, serverIp, serverPort);
                socketProducerCSVReader.run();
                Thread.sleep(5);
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
