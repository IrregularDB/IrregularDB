package sources;


import records.DataPoint;
import records.TimeSeriesReading;
import scheduling.WorkingSet;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class SocketDataReceiver extends DataReceiver {

    private DataInputStream clientInputStream;

    public SocketDataReceiver(WorkingSet workingSet, Socket clientConnection) {
        super(workingSet);
        try {
            this.clientInputStream = new DataInputStream(clientConnection.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void receiveData() {
        while (true) {
            sendTimeSeriesReadingToBuffer(getTimeSeriesReadingFromSocket());
        }
    }

    private TimeSeriesReading getTimeSeriesReadingFromSocket() {
        try {
            long timestamp = clientInputStream.readLong();
            float value = clientInputStream.readFloat();

            int amountOfBytesToReadAsTag = clientInputStream.readInt();
            byte[] bytes = clientInputStream.readNBytes(amountOfBytesToReadAsTag);
            String timeSeriesTag = new String(bytes, StandardCharsets.UTF_8);

            return new TimeSeriesReading(timeSeriesTag, new DataPoint(timestamp, value));
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("Exception while reading from socket");
    }
}
