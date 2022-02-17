package compression.timestamp;

import compression.value.PMCMeanValueCompressionModel;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

// TODO: add tests with error-bound different than ZERO for regular time stamp compression

class RegularTimeStampCompressionModelTest {
    RegularTimeStampCompressionModel regularModel;

    @BeforeEach
    void init() {
        double errorBound = 0;
        regularModel = new RegularTimeStampCompressionModel();
    }

    // We expect to be able to append any two data points no matter how different as then we have not SI
    @Test
    void appendTwoTimeStamps() {
        Assertions.assertTrue(regularModel.appendTimeStamp(0));
        Assertions.assertTrue(regularModel.appendTimeStamp(1000000));
    }

    @Test
    void appendThreeTimeStampsWithSameSI() {
        Assertions.assertTrue(regularModel.appendTimeStamp(0));
        Assertions.assertTrue(regularModel.appendTimeStamp(100));
        Assertions.assertTrue(regularModel.appendTimeStamp(200));
    }

    @Test
    void appendThreeTimeStampsDifferentSI() {
        Assertions.assertTrue(regularModel.appendTimeStamp(0));
        Assertions.assertTrue(regularModel.appendTimeStamp(100));
        Assertions.assertFalse(regularModel.appendTimeStamp(999));
    }

    @Test
    void appendAfterFailedAppendNotAllowed() {
        Assertions.assertTrue(regularModel.appendTimeStamp(0));
        Assertions.assertTrue(regularModel.appendTimeStamp(100));
        Assertions.assertFalse(regularModel.appendTimeStamp(999));

        Assertions.assertThrows(IllegalArgumentException.class, () -> regularModel.appendTimeStamp(200));
    }

    @Test
    void getLength() {
        assertEquals(0, regularModel.getLength());
        regularModel.appendTimeStamp(0);
        assertEquals(1, regularModel.getLength());
        regularModel.appendTimeStamp(100);
        regularModel.appendTimeStamp(200);
        assertEquals(3, regularModel.getLength());
    }


    @Test
    void resetAndAppendAllEmptyModel() {
        // We test that we can insert three legal points at once on an empty model
        List<Long> timeStamps = Arrays.asList(0L, 100L, 200L, 300L);
        Assertions.assertTrue(regularModel.resetAndAppendAll(timeStamps));
    }

    @Test
    void resetAndAppendAllNonEmptyModel() {
        // We test that we can insert three legal points at once even though we inserted something different earlier
        // as this call should also reset the model
        List<Long> timeStamps = Arrays.asList(0L, 100L, 200L, 300L);
        regularModel.resetAndAppendAll(timeStamps);

        timeStamps = Arrays.asList(0L, 200L, 400L, 600L);
        Assertions.assertTrue(regularModel.resetAndAppendAll(timeStamps));
    }

    @Test
    void getTimeStampBlobEmptyModel() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> regularModel.getTimeStampBlob());
    }

    @Test
    void getTimeStampBlob() {
        List<Long> timeStamps = Arrays.asList(0L, 100L, 200L, 300L);
        regularModel.resetAndAppendAll(timeStamps);

        ByteBuffer timeStampBlob = regularModel.getTimeStampBlob();
        int si = timeStampBlob.getInt(0);
        assertEquals(100, si);
    }

    @Test
    void getCompressionRatio0DataPoints() {
        Assertions.assertThrows(UnsupportedOperationException.class, () -> regularModel.getCompressionRatio());
    }

    @Test
    void getCompressionRatio2DataPoints() {
        // We expect that we have used 4 bytes to represent 2 data points so we get 2/4 = 0.5
        List<Long> timeStamps = Arrays.asList(0L, 100L);
        regularModel.resetAndAppendAll(timeStamps);
        assertEquals(0.5, regularModel.getCompressionRatio());
    }

    @Test
    void getCompressionRatio4DataPoints() {
        // We expect that we have used 4 bytes to represent 4 data points so we get 4/4 = 1
        List<Long> timeStamps = Arrays.asList(0L, 100L, 200L, 300L);
        regularModel.resetAndAppendAll(timeStamps);
        assertEquals(1, regularModel.getCompressionRatio());
    }

    @Test
    void getCompressionRatio8DataPoints() {
        // We expect that we have used 4 bytes to represent 8 data points so we get 8/4 = 1
        List<Long> timeStamps = Arrays.asList(0L, 100L, 200L, 300L, 400L, 500L, 600L, 700L);
        regularModel.resetAndAppendAll(timeStamps);
        assertEquals(2, regularModel.getCompressionRatio());
    }

    @Test
    void reduceToSizeN() {
        // TODO: implement reduce to N test
    }
}