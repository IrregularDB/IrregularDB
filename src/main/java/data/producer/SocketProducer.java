package data.producer;

import records.TimeSeriesReading;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class SocketProducer {
    private final List<TimeSeriesReading> timeSeriesReadings;
    private final String serverIp;
    private final int serverPort;
    private DataOutputStream dataOutputStream;

    public SocketProducer(List<TimeSeriesReading> timeSeriesReadings, String serverIp, int serverPort) {
        this.timeSeriesReadings = timeSeriesReadings;
        this.serverIp = serverIp;
        this.serverPort = serverPort;
    }

    public void connectAndSendData(){
        try {
            Socket socket = new Socket(serverIp, serverPort);
            this.dataOutputStream = new DataOutputStream(socket.getOutputStream());
            for (TimeSeriesReading timeSeriesReading : timeSeriesReadings) {
                writeTimeSeriesReadingToSocket(timeSeriesReading);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeTimeSeriesReadingToSocket(TimeSeriesReading timeSeriesReading) throws IOException {
        dataOutputStream.writeLong(timeSeriesReading.dataPoint().timestamp());
        dataOutputStream.writeFloat(timeSeriesReading.dataPoint().value());

        byte[] timeSeriesTagAsBytes = timeSeriesReading.tag().getBytes(StandardCharsets.UTF_8);
        dataOutputStream.writeInt(timeSeriesTagAsBytes.length);
        dataOutputStream.write(timeSeriesTagAsBytes);
    }
}
