package data.producer;

import records.DataPoint;
import records.TimeSeriesReading;
import sources.SocketDataReceiver;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static sources.SocketDataReceiver.INDICATE_END_OF_STREAM;

public class SocketProducer {
    private final List<TimeSeriesReading> timeSeriesReadings;
    private final String serverIp;
    private final int serverPort;

    public SocketProducer(List<TimeSeriesReading> timeSeriesReadings, String serverIp, int serverPort) {
        this.timeSeriesReadings = timeSeriesReadings;
        this.serverIp = serverIp;
        this.serverPort = serverPort;
    }

    public void connectAndSendData(){
        try {
            Socket socket = new Socket(serverIp, serverPort);
            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
            String lastTag = null;
            for (TimeSeriesReading timeSeriesReading : timeSeriesReadings) {
                if (!timeSeriesReading.getTag().equals(lastTag)) {
                    lastTag = timeSeriesReading.getTag();
                    writeFullTimeSeriesReadingToSocket(timeSeriesReading, dataOutputStream);
                } else {
                    writeOnlyDataPointToSocket(timeSeriesReading.getDataPoint(), dataOutputStream);
                }
            }
            dataOutputStream.writeByte(INDICATE_END_OF_STREAM);
            Thread.sleep(100);
            dataOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void writeFullTimeSeriesReadingToSocket(TimeSeriesReading reading, DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.write(SocketDataReceiver.INDICATES_NEW_TAG);
        byte[] timeSeriesTagAsBytes = reading.getTag().getBytes(StandardCharsets.UTF_8);
        dataOutputStream.writeInt(timeSeriesTagAsBytes.length);
        dataOutputStream.write(timeSeriesTagAsBytes);

        dataOutputStream.writeLong(reading.getDataPoint().timestamp());
        dataOutputStream.writeFloat(reading.getDataPoint().value());
    }

    private void writeOnlyDataPointToSocket(DataPoint dataPoint, DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.write(SocketDataReceiver.INDICATES_NO_NEW_TAG);
        dataOutputStream.writeLong(dataPoint.timestamp());
        dataOutputStream.writeFloat(dataPoint.value());
    }
}
