import records.TimeSeriesReading;
import sources.CSVDataReceiver;

import java.util.concurrent.ConcurrentLinkedQueue;

public class Main {

    private static ConcurrentLinkedQueue<TimeSeriesReading> MAIN_TIMESERIES_RECORDING_BUFFER;


    public static void main(String[] args) {
        initializeMainBuffer();
        initializeDataReceivers();

        try {
            Thread.sleep(3000);
            System.out.println(MAIN_TIMESERIES_RECORDING_BUFFER.poll());
            System.out.println(MAIN_TIMESERIES_RECORDING_BUFFER.poll());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
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
