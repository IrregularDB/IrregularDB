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
    public static final byte INDICATE_END_OF_STREAM = 0b01010101;


    private DataInputStream clientInputStream;
    private String currentInUseTag;

    public SocketDataReceiver(WorkingSet workingSet, Socket socket) {
        super(workingSet);
        try {
            // TODO: make this maybe into a buffered reader.
            this.clientInputStream = new DataInputStream(socket.getInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void receiveData() {
        try {
            while (true) {
                TimeSeriesReading timeSeriesReadingFromSocket = getTimeSeriesReadingFromSocket();
                if (timeSeriesReadingFromSocket != null) {
                    sendTimeSeriesReadingToBuffer(timeSeriesReadingFromSocket);
                } else {
                    break;
                }
            }
        } catch (IOException e) {
            // We don't rethrow the exception as we want to close the socket if this happens and not the crash the system.
            System.out.println("Socket was terminated unexpectedly.");
        }
        close();
    }

    private TimeSeriesReading getTimeSeriesReadingFromSocket() throws IOException {
        byte controlByte = clientInputStream.readByte();

        if (controlByte == INDICATE_END_OF_STREAM) {
            return null;
        }
        if (controlByte == INDICATES_NEW_TAG) {
            this.currentInUseTag = readTagFromStream();
        }
        return new TimeSeriesReading(this.currentInUseTag, readDataPointFromStream());
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
