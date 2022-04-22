package data.producer;

import records.DataPoint;
import records.TimeSeriesReading;
import sources.SocketDataReceiver;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static sources.SocketDataReceiver.INDICATE_END_OF_STREAM;

public class SocketProducerCSVReader {

    public static void run(File csvFile, String serverIp, int serverPort){
        if (!csvFile.exists()) {
            System.out.println("Socket Producer CSVReader can not find file with path: " + csvFile.getAbsolutePath());
            return;
        }
//        new Thread(() -> connectAndSendData(csvFile, serverIp, serverPort)).start();
    }
//
//
//    private static void connectAndSendData(File csvFile, String serverIp, int serverPort){
//        try {
//            Socket socket = new Socket(serverIp, serverPort);
//            DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
//
//
//            String lastTag = null;
//            for (TimeSeriesReading timeSeriesReading : timeSeriesReadings) {
//                if (!timeSeriesReading.getTag().equals(lastTag)) {
//                    lastTag = timeSeriesReading.getTag();
//                    writeFullTimeSeriesReadingToSocket(timeSeriesReading, dataOutputStream);
//                } else {
//                    writeOnlyDataPointToSocket(timeSeriesReading.getDataPoint(), dataOutputStream);
//                }
//            }
//            dataOutputStream.writeByte(INDICATE_END_OF_STREAM);
//            Thread.sleep(100);
//            dataOutputStream.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//
//
//    private void writeFullTimeSeriesReadingToSocket(TimeSeriesReading reading, DataOutputStream dataOutputStream) throws IOException {
//        dataOutputStream.write(SocketDataReceiver.INDICATES_NEW_TAG);
//        byte[] timeSeriesTagAsBytes = reading.getTag().getBytes(StandardCharsets.UTF_8);
//        dataOutputStream.writeInt(timeSeriesTagAsBytes.length);
//        dataOutputStream.write(timeSeriesTagAsBytes);
//
//        dataOutputStream.writeLong(reading.getDataPoint().timestamp());
//        dataOutputStream.writeFloat(reading.getDataPoint().value());
//    }
//
//    private void writeOnlyDataPointToSocket(DataPoint dataPoint, DataOutputStream dataOutputStream) throws IOException {
//        dataOutputStream.write(SocketDataReceiver.INDICATES_NO_NEW_TAG);
//        dataOutputStream.writeLong(dataPoint.timestamp());
//        dataOutputStream.writeFloat(dataPoint.value());
//    }
//
}