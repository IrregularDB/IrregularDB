package data.producer;

import config.ConfigProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import records.DataPoint;
import records.TimeSeriesReading;
import scheduling.Partitioner;
import scheduling.WorkingSet;
import sources.SocketDataReceiverSpawner;
import storage.TestDatabaseConnectionFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class SocketProducerTest {

    private static List<TimeSeriesReading> getNTestDataForNTags(List<String> tags, int n) {
        return tags.stream()
                .flatMap(tag -> getNTestDataForTag(tag, n).stream())
                .collect(Collectors.toList());
    }

    private static List<TimeSeriesReading> getNTestDataForTag(String tag, int n) {
        List<TimeSeriesReading> data = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            data.add(getTestData(tag, i));
        }
        return data;
    }

    private static TimeSeriesReading getTestData(String tag, int seed) {
        return new TimeSeriesReading(tag, new DataPoint(seed, seed));
    }

    @Test
    void connectAndSendData() {
        int serverSocketPort = ConfigProperties.getInstance().getSocketDataReceiverSpawnerPort();
        Queue<TimeSeriesReading> workingSetBuffer = new ConcurrentLinkedQueue<>();
        TestPartitioner testPartitioner = new TestPartitioner(workingSetBuffer);
        SocketDataReceiverSpawner socketDataReceiverSpawner = new SocketDataReceiverSpawner(testPartitioner, serverSocketPort);
        socketDataReceiverSpawner.spawn();

        List<TimeSeriesReading> testData = getNTestDataForTag("key1", 10);
        SocketProducer socketProducer = new SocketProducer(testData, "localhost", serverSocketPort);
        socketProducer.connectAndSendData();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assertions.assertEquals(testData.size(), workingSetBuffer.size());

        for (int i = 0; i < testData.size(); i++) {
            Assertions.assertEquals(testData.get(i), workingSetBuffer.poll());
        }
    }

    @Test
    void connectAndSendDataSeveralTagsInOneStream() {
        int serverSocketPort = ConfigProperties.getInstance().getSocketDataReceiverSpawnerPort() + 1;
        Queue<TimeSeriesReading> workingSetBuffer = new ConcurrentLinkedQueue<>();
        TestPartitioner testPartitioner = new TestPartitioner(workingSetBuffer);
        SocketDataReceiverSpawner socketDataReceiverSpawner = new SocketDataReceiverSpawner(testPartitioner, serverSocketPort);
        socketDataReceiverSpawner.spawn();

        List<TimeSeriesReading> testData1 = getNTestDataForTag("key1", 5);
        List<TimeSeriesReading> testData2 = getNTestDataForTag("key2", 5);
        List<TimeSeriesReading> testData3 = getNTestDataForTag("key1", 5);
        List<TimeSeriesReading> allTestData = Stream.concat(Stream.concat(testData1.stream(), testData2.stream()), testData3.stream()).toList();
        SocketProducer socketProducer = new SocketProducer(allTestData, "localhost", serverSocketPort);
        socketProducer.connectAndSendData();

        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assertions.assertEquals(allTestData.size(), workingSetBuffer.size());

        for (int i = 0; i < allTestData.size(); i++) {
            Assertions.assertEquals(allTestData.get(i), workingSetBuffer.poll());
        }
    }

    private static class TestPartitioner extends Partitioner {

        private Queue<TimeSeriesReading> buffer;

        public TestPartitioner(Queue<TimeSeriesReading> buffer) {
            super(null, -1);
            this.buffer = buffer;
        }

        @Override
        public WorkingSet workingSetToSpawnReceiverFor() {
            return new WorkingSet(this.buffer, null, new TestDatabaseConnectionFactory());
        }
    }
}