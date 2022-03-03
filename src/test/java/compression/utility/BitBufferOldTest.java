package compression.utility;

import compression.utility.BitBuffer.BitBuffer;
import compression.utility.BitBuffer.BitBufferOld;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class BitBufferOldTest {
    BitBuffer bitBuffer;

    private String removeSpace(String string) {
        return string.replace(" ", "");
    }

    @BeforeEach
    void beforeEach() {
        bitBuffer = new BitBufferOld(true);
    }

    @Test
    void parseByte() {
        // This is not really a test of our code but how parse byte works
        // as we can see from this test it append zeroes in the FRONT
        byte b = (byte) Integer.parseInt("1", 2);
        assertEquals(1, b);

        b = (byte) Integer.parseInt("01111111", 2);
        assertEquals(127, b);

        b = (byte) Integer.parseInt("10000000", 2);
        assertEquals(-128, b);

        b = (byte) Integer.parseInt("11111111", 2);
        assertEquals(-1, b);
    }

    @Test
    void putOneRawInt() {
        bitBuffer.writeRawInt(1);

        ByteBuffer byteBuffer = bitBuffer.getFinishedByteBuffer();
        Assertions.assertEquals(1, byteBuffer.getInt(0));

        // We also expect it to automatically shrink the byte buffer to size 4
        Assertions.assertEquals(4, byteBuffer.capacity());
    }

    @Test
    void putFiveRawInts() {
        // We test that the buffer automatically extends with 4 more bytes to allow us to store multiple doubles
        bitBuffer.writeRawInt(1);
        bitBuffer.writeRawInt(2);
        bitBuffer.writeRawInt(3);
        bitBuffer.writeRawInt(4);
        bitBuffer.writeRawInt(5);


        ByteBuffer byteBuffer = bitBuffer.getFinishedByteBuffer();
        Assertions.assertEquals(20, byteBuffer.capacity());
        Assertions.assertEquals(1, byteBuffer.getInt(0));
        Assertions.assertEquals(2, byteBuffer.getInt(4));
        Assertions.assertEquals(3, byteBuffer.getInt(8));
        Assertions.assertEquals(4, byteBuffer.getInt(12));
        Assertions.assertEquals(5, byteBuffer.getInt(16));
    }

    @Test
    void writeFalseBit() {
        // We check that writing false bit 8 times creates the byte 0000 0000 (which is equivalent to 0)
        for (int i = 0; i < 8; i++) {
            bitBuffer.writeFalseBit();
        }
        ByteBuffer byteBuffer = bitBuffer.getFinishedByteBuffer();
        Assertions.assertEquals(0, byteBuffer.get(0));
    }

    @Test
    void writeTrueBit() {
        // we check that writing true bit 8 times creates the byte 1111 1111 (which is equivalent to -1)
        for (int i = 0; i < 8; i++) {
            bitBuffer.writeTrueBit();
        }
        ByteBuffer byteBuffer = bitBuffer.getFinishedByteBuffer();
        Assertions.assertEquals(-1, byteBuffer.get(0));
    }

    @Test
    void writeIntUsingNBitsFor9BitValue() {
        String bitPattern = removeSpace("0000 0000 1");
        bitBuffer.writeIntUsingNBits(BitUtil.bits2Int(bitPattern), 9);

        // We expect it to create two bytes:
        //  - 1000 0000 (equivalent to 0)
        //  - 1111 1111 (equivalent to -1)
        ByteBuffer byteBuffer = bitBuffer.getFinishedByteBuffer();
        Assertions.assertEquals(0, byteBuffer.get(0));
        Assertions.assertEquals(-1, byteBuffer.get(1));
    }

    @Test
    void writeIntUsingNBitsWithNLessThanSize() {
        // We write the following 8 bit value using only 2 bits
        String bitPattern = removeSpace("0000 0001");
        bitBuffer.writeIntUsingNBits(BitUtil.bits2Int(bitPattern), 2);

        // We then expect it to have only 01 in the buffer, which is the filled out with 1's after it giving
        // 0111 1111 (which is equivalent to 127)
        ByteBuffer byteBuffer = bitBuffer.getFinishedByteBuffer();
        Assertions.assertEquals(127, byteBuffer.get(0));
    }

    @Test
    void handledUnfinishedByte() {
        bitBuffer.writeFalseBit();

        // We expect it to finish the current byte with 1's so we get 0111 1111 which is equivalent to
        ByteBuffer byteBuffer = bitBuffer.getFinishedByteBuffer();
        Assertions.assertEquals(127, byteBuffer.get(0));
    }
}