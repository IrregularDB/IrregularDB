package data.producer;

import config.ConfigProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import records.FinalizeTimeSeriesReading;
import records.TimeSeriesReading;
import sources.CSVTimeSeriesReader;
import sources.SocketDataReceiverSpawner;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

class SocketProducerCSVReaderTest {

    @BeforeAll
    public static void setup(){
        ConfigProperties.isTest = true;
    }

    @Test
    void connectAndSendData() throws IOException, InterruptedException {
        String csvFilePath = "src\\test\\resources\\testFolder\\csvDocketProducer.csv";
        String serverIp = "localhost";
        int serverPort = ConfigProperties.getInstance().getSocketDataReceiverSpawnerPort();

        Queue<TimeSeriesReading> workingSetBuffer = new ConcurrentLinkedQueue<>();
        TestPartitioner testPartitioner = new TestPartitioner(workingSetBuffer);

        SocketDataReceiverSpawner socketDataReceiverSpawner = new SocketDataReceiverSpawner(testPartitioner, serverPort);
        socketDataReceiverSpawner.spawn();

        SocketProducerCSVReader socketProducerCSVReader = new SocketProducerCSVReader(new File(csvFilePath), ",", serverIp, serverPort);
        Thread csvProducerThread = socketProducerCSVReader.run();
        csvProducerThread.join();

        CSVTimeSeriesReader csvTimeSeriesReader = new CSVTimeSeriesReader(new File(csvFilePath), ","); // create a new instance as the other is already consumed
        TimeSeriesReading next = csvTimeSeriesReader.next();
        List<TimeSeriesReading> expectedReadings = new ArrayList<>();
        while (next != null) {
            expectedReadings.add(next);
            next = csvTimeSeriesReader.next();
        }
        expectedReadings.add(new FinalizeTimeSeriesReading("tag1"));
        expectedReadings.add(new FinalizeTimeSeriesReading("tag2"));

        Assertions.assertEquals(expectedReadings.size(), workingSetBuffer.size());
        for (int i = 0; i < expectedReadings.size(); i++) {
            Assertions.assertEquals(expectedReadings.get(i), workingSetBuffer.poll());
        }
    }
}