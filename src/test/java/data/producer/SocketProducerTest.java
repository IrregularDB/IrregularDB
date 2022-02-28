package data.producer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import records.DataPoint;
import records.TimeSeriesReading;
import scheduling.Partitioner;
import scheduling.WorkingSet;
import sources.SocketDataReceiverSpawner;
import storage.TestDatabaseConnection;
import storage.TestDatabaseConnectionFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

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
        Queue<TimeSeriesReading> workingSetBuffer = new ConcurrentLinkedQueue<>();
        TestPartitioner testPartitioner = new TestPartitioner(workingSetBuffer);
        SocketDataReceiverSpawner socketDataReceiverSpawner = new SocketDataReceiverSpawner(testPartitioner);
        socketDataReceiverSpawner.spawn();


        List<TimeSeriesReading> testData = getNTestDataForTag("key1", 10);
        SocketProducer socketProducer = new SocketProducer(testData, "localhost", 4672);
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