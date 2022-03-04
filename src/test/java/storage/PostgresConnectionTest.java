package storage;

import compression.utility.BitUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import records.Segment;
import java.nio.ByteBuffer;


class PostgresConnectionTest {

    private final PostgresConnection postgresConnection = new PostgresConnection();


    //@Test
    public void testDBInsertsTimeSeries(){
        int id1 = postgresConnection.getTimeSeriesId("key1");
        int id2 = postgresConnection.getTimeSeriesId("key2");
        int id3 = postgresConnection.getTimeSeriesId("key1");

        Assertions.assertEquals(1, id1);
        Assertions.assertEquals(2, id2);
        Assertions.assertEquals(1, id3);
    }

    //@Test
    public void testDBInsertsSegment(){
        Segment segment = new Segment(1, 111L,222L,1, ByteBuffer.allocate(0), 1, ByteBuffer.allocate(0));
        this.postgresConnection.insertSegment(segment);
    }


    @Test
    public void TestOfCombinedModelTypes() {
        int valueModelType = 5;
        int timestampType = 2;
        short combined = PostgresConnection.combineTwoModelTypes(valueModelType, timestampType);
        PostgresConnection.ValueTimeStampModelPair seperated = PostgresConnection.combinedModelTypesToIndividual(combined);

        Assertions.assertEquals(valueModelType, seperated.valueModelType);
        Assertions.assertEquals(timestampType, seperated.timeStampModelType);
    }

}