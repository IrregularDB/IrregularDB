import records.TimeSeriesReading;
import scheduling.MapPartitioner;
import scheduling.Partitioner;
import scheduling.WorkingSetFactory;
import sources.CSVDataReceiver;

import java.util.concurrent.ConcurrentLinkedQueue;

public class Main {

    private static ConcurrentLinkedQueue<TimeSeriesReading> MAIN_TIMESERIES_RECORDING_BUFFER;


    public static void main(String[] args) {
        initializeMainBuffer();
        initializeDataReceivers();

        // TODO: Should be configurable which partitioner should be used
        Partitioner mapPartitioner = new MapPartitioner(new WorkingSetFactory(), MAIN_TIMESERIES_RECORDING_BUFFER);
        mapPartitioner.performPartitioning();
    }


    private static void initializeMainBuffer(){
        MAIN_TIMESERIES_RECORDING_BUFFER = new ConcurrentLinkedQueue<>();
    }


    private static void initializeDataReceivers() {
        if (MAIN_TIMESERIES_RECORDING_BUFFER == null) {
            throw new IllegalStateException("buffer must be initialized before initializing data receivers");
        }

        CSVDataReceiver csvDataReceiver = new CSVDataReceiver("src/main/resources/test.csv", MAIN_TIMESERIES_RECORDING_BUFFER, ",");

        csvDataReceiver.run();
    }
}
