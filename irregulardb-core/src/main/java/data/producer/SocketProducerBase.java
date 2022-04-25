package data.producer;

import records.DataPoint;
import records.TimeSeriesReading;
import sources.SocketDataReceiver;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

import static sources.SocketDataReceiver.INDICATE_END_OF_STREAM;

public abstract class SocketProducerBase {
    private final Socket socket;
    private final BufferedOutputStream dataOutputStream;
    private String lastTag;

    public SocketProducerBase(String serverIp, int serverPort) throws IOException {
        this.socket = new Socket(serverIp, serverPort);
        this.dataOutputStream = new BufferedOutputStream(new DataOutputStream(socket.getOutputStream()));
        this.lastTag = "";
    }

    public final Thread run(){
        Thread thread = new Thread(this::connectAndSendData);
        thread.start();
        return thread;
    }
    protected abstract void connectAndSendData();

    protected void sendReadingToStream(TimeSeriesReading timeSeriesReading) throws IOException {
        if (timeSeriesReading.getTag().equals(lastTag)) {
            writeOnlyDataPointToSocket(timeSeriesReading.getDataPoint());
        } else {
            lastTag = timeSeriesReading.getTag();
            writeFullTimeSeriesReadingToSocket(timeSeriesReading);
        }
    }

    private void writeFullTimeSeriesReadingToSocket(TimeSeriesReading reading) throws IOException {
        byte[] timeSeriesTagAsBytes = reading.getTag().getBytes(StandardCharsets.UTF_8);
        // We need to write: BYTE + INT + STRING + LONG + FLOAT
        int amtBytesToWrite = 1 + 4 + timeSeriesTagAsBytes.length + 8 + 4;
        ByteBuffer outputBuffer = ByteBuffer.allocate(amtBytesToWrite);

        outputBuffer.put(SocketDataReceiver.INDICATES_NEW_TAG);

        outputBuffer.putInt(timeSeriesTagAsBytes.length);
        outputBuffer.put(timeSeriesTagAsBytes);

        outputBuffer.putLong(reading.getDataPoint().timestamp());
        outputBuffer.putFloat(reading.getDataPoint().value());
        dataOutputStream.write(outputBuffer.array(), 0, amtBytesToWrite);
    }

    private void writeOnlyDataPointToSocket(DataPoint dataPoint) throws IOException {
        // We need to write:  BYTE + LONG + FLOAT
        final int amtBytesToWrite = 1 + 8 + 4;
        ByteBuffer outputBuffer = ByteBuffer.allocate(amtBytesToWrite);

        outputBuffer.put(SocketDataReceiver.INDICATES_NO_NEW_TAG);
        outputBuffer.putLong(dataPoint.timestamp());
        outputBuffer.putFloat(dataPoint.value());
        dataOutputStream.write(outputBuffer.array(), 0, amtBytesToWrite);
    }

    protected void close() throws IOException {
        dataOutputStream.write(INDICATE_END_OF_STREAM);
        dataOutputStream.close();
        socket.close();
    }
}
