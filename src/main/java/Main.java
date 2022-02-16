import records.TimeSeriesReading;
import sources.CSVDataReceiver;

import java.util.concurrent.ConcurrentLinkedQueue;

public class Main {

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

}
