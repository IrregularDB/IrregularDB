package sources;


import records.DataPoint;
import records.TimeSeriesReading;
import scheduling.WorkingSet;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class SocketDataReceiver extends DataReceiver {

    public static final byte INDICATES_NEW_TAG = 0b00000001;
    public static final byte INDICATES_NO_NEW_TAG = 0b00000000;


    private DataInputStream clientInputStream;
    private String currentInUseTag;

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
            if (streamReadingContainsANewTag()) {
                this.currentInUseTag = readTagFromStream();
            }
            return new TimeSeriesReading(this.currentInUseTag, readDataPointFromStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("Exception while reading from socket");
    }

    private boolean streamReadingContainsANewTag() throws IOException {
            return clientInputStream.readByte() == INDICATES_NEW_TAG;
    }

    private String readTagFromStream() throws IOException {
        int amountOfBytesToReadAsTag = clientInputStream.readInt();
        byte[] bytes = clientInputStream.readNBytes(amountOfBytesToReadAsTag);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private DataPoint readDataPointFromStream() throws IOException {
            long timestamp = clientInputStream.readLong();
            float value = clientInputStream.readFloat();
            return new DataPoint(timestamp, value);
    }
}
