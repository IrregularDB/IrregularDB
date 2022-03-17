package sources;

import config.ConfigProperties;
import scheduling.Partitioner;
import scheduling.WorkingSet;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketDataReceiverSpawner extends DataReceiverSpawner {

    private final int serverSocketPort;

    public SocketDataReceiverSpawner(Partitioner partitioner, int serverSocketPort) {
        super(partitioner);
        this.serverSocketPort = serverSocketPort;
    }

    public SocketDataReceiverSpawner(Partitioner partitioner) {
        super(partitioner);
        this.serverSocketPort = ConfigProperties.INSTANCE.getSocketDataReceiverSpawnerPort();
    }

    @Override
    public void spawn() {
        new Thread(this::runServerSocket).start();
    }

    private void runServerSocket() {
        try {
            ServerSocket serverSocket = new ServerSocket(this.serverSocketPort);
            while (true) {
                Socket connection = serverSocket.accept();
                WorkingSet workingSet = partitioner.workingSetToSpawnReceiverFor();
                runReceiverInThread(new SocketDataReceiver(workingSet, connection));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
