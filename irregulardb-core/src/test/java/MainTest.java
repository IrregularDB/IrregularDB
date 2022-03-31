import config.ConfigProperties;
import scheduling.IncrementPartitioner;
import scheduling.Partitioner;
import scheduling.WorkingSetFactory;
import sources.CSVDataReceiverSpawner;
import sources.DataReceiverSpawner;

class MainTest {

//    @Test
    /**
     * This test is currently dependant on the actual run configuration. The sources used are for reference given here
     * source.csv= src/test/resources/integration-test/01.csv ,\
     *   src/test/resources/integration-test/02.csv ,\
     *   src/test/resources/integration-test/03.csv ,\
     *   src/test/resources/integration-test/04.csv ,\
     *   src/test/resources/integration-test/05.csv ,\
     *   src/test/resources/integration-test/06.csv ,\
     *   src/test/resources/integration-test/07.csv ,\
     *   src/test/resources/integration-test/08.csv ,\
     *   src/test/resources/integration-test/09.csv ,\
     *   src/test/resources/integration-test/10.csv
     */
    public void integrationTest() {
        Partitioner partitioner = new IncrementPartitioner(new WorkingSetFactory(), ConfigProperties.getInstance().getConfiguredNumberOfWorkingSets());

        DataReceiverSpawner dataReceiverSpawner = new CSVDataReceiverSpawner(partitioner, ConfigProperties.getInstance().getCsvSources());
        dataReceiverSpawner.spawn();
    }

}