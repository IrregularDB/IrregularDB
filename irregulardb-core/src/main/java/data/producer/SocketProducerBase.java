package data.producer;

import records.DataPoint;
import records.TimeSeriesReading;
import sources.SocketDataReceiver;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static sources.SocketDataReceiver.INDICATE_END_OF_STREAM;

public abstract class SocketProducerBase {
    private final Socket socket;
    private final DataOutputStream dataOutputStream;
    private String lastTag = "";

    public SocketProducerBase(String serverIp, int serverPort) throws IOException {
        this.socket = new Socket(serverIp, serverPort);
        this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
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

    protected void close() throws IOException {
        dataOutputStream.writeByte(INDICATE_END_OF_STREAM);
        dataOutputStream.close();
        socket.close();
    }

    private void writeFullTimeSeriesReadingToSocket(TimeSeriesReading reading) throws IOException {
        dataOutputStream.writeByte(SocketDataReceiver.INDICATES_NEW_TAG);
        byte[] timeSeriesTagAsBytes = reading.getTag().getBytes(StandardCharsets.UTF_8);
        dataOutputStream.writeInt(timeSeriesTagAsBytes.length);
        dataOutputStream.write(timeSeriesTagAsBytes);

        dataOutputStream.writeLong(reading.getDataPoint().timestamp());
        dataOutputStream.writeFloat(reading.getDataPoint().value());
    }

    private void writeOnlyDataPointToSocket(DataPoint dataPoint) throws IOException {
        dataOutputStream.writeByte(SocketDataReceiver.INDICATES_NO_NEW_TAG);
        dataOutputStream.writeLong(dataPoint.timestamp());
        dataOutputStream.writeFloat(dataPoint.value());
    }

}
