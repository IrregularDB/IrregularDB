package storage;

import org.junit.Ignore;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import records.Segment;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class PostgresConnectionTest {

    private final PostgresConnection postgresConnection = new PostgresConnection();


    @Test
    @Ignore
    public void testDBInsertsTimeSeries(){
        int id1 = postgresConnection.getTimeSeriesId("key1");
        int id2 = postgresConnection.getTimeSeriesId("key2");
        int id3 = postgresConnection.getTimeSeriesId("key1");

        Assertions.assertEquals(1, id1);
        Assertions.assertEquals(2, id2);
        Assertions.assertEquals(1, id3);
    }

    @Test
    @Ignore
    public void testDBInsertsSegment(){
        Segment segment = new Segment(1, 111L,222L,1, ByteBuffer.allocate(0), 1, ByteBuffer.allocate(0));
        this.postgresConnection.insertSegment(segment);
    }

}