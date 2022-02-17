package sources;

import scheduling.Partitioner;

public abstract class DataReceiverSpawner {

    protected final Partitioner partitioner;

    public DataReceiverSpawner(Partitioner partitioner){
        this.partitioner = partitioner;
    }

    /**
     * This method will spawn threads that receives data
     */
    public abstract void spawn();

    protected static void runReceiverInThread(DataReceiver receiver){
        Thread thread = new Thread(receiver::receiveData);
        thread.start();
    }
}
