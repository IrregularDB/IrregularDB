package sources;

import scheduling.Partitioner;

import java.util.function.Function;

public abstract class DataReceiverSpawner {

    protected final Partitioner partitioner;

    public DataReceiverSpawner(Partitioner partitioner){
        this.partitioner = partitioner;
    }

    /**
     * This method will spawn threads that receives data
     */
    public abstract void spawn();

    protected void runInThread(Runnable functionToExecute){
        Thread thread = new Thread(functionToExecute);
        thread.start();
    }
}
