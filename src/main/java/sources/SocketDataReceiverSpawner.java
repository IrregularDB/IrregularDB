package sources;

import config.ConfigProperties;
import scheduling.Partitioner;
import scheduling.WorkingSet;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketDataReceiverSpawner extends DataReceiverSpawner {

    private ServerSocket serverSocket;

    public SocketDataReceiverSpawner(Partitioner partitioner) {
        super(partitioner);
    }

    @Override
    public void spawn() {
        new Thread(this::runServerSocket).start();
    }

    private void runServerSocket() {
        try {
            this.serverSocket = new ServerSocket(ConfigProperties.INSTANCE.getSocketDataReceiverSpawnerPort());
            while (true) {
                Socket connection = this.serverSocket.accept();
                WorkingSet workingSet = partitioner.workingSetToSpawnReceiverFor();
                runReceiverInThread(new SocketDataReceiver(workingSet, connection));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
