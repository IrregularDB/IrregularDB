package storage;

import compression.utility.ModelTypeUtil;
import config.ConfigProperties;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import records.Segment;
import records.ValueTimeStampModelPair;

import java.nio.ByteBuffer;


class PostgresConnectionTest {

    @BeforeAll
    public static void setupConfig(){
        ConfigProperties.isTest = true;
    }

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
        byte valueModelTypeId = 0b1;
        byte timeStampModelTypeId = 0b1;
        Segment segment = new Segment(1, 111L,222L, valueModelTypeId, ByteBuffer.allocate(0), timeStampModelTypeId, ByteBuffer.allocate(0));
        this.postgresConnection.insertSegment(segment);
    }


    @Test
    public void TestOfCombinedModelTypes() {
        byte valueModelType = 5;
        byte timestampType = 2;
        short combined = ModelTypeUtil.combineTwoModelTypes(valueModelType, timestampType);
        ValueTimeStampModelPair modelPair = ModelTypeUtil.combinedModelTypesToIndividual(combined);

        Assertions.assertEquals(valueModelType, modelPair.valueModelType());
        Assertions.assertEquals(timestampType, modelPair.timeStampModelType());
    }

    @Test
    public void TestOfCombinedModelTypesWithNegativeValueModelType() {
        byte valueModelType = -1;
        byte timestampModelType = 127;

        Assertions.assertThrows(IllegalArgumentException.class, () -> ModelTypeUtil.combineTwoModelTypes(valueModelType, timestampModelType));
    }

    @Test
    public void TestOfCombinedModelTypesWithNegativeTimeStampModelType() {
        byte valueModelType = 127;
        byte timestampModelType = -1;
        Assertions.assertThrows(IllegalArgumentException.class, () -> ModelTypeUtil.combineTwoModelTypes(valueModelType, timestampModelType));
    }

}