package compression.utility;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class BitBufferOldTest {
    BitBufferOld bitBuffer;

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
    void putOneNumberValue() {
        bitBuffer.writeRawInt(1);

        // We test that the buffer automatically extends to 4 bytes to store a float
        ByteBuffer byteBuffer = bitBuffer.getFinishedByteBuffer();
        Assertions.assertEquals(4, byteBuffer.capacity());
        Assertions.assertEquals(1, byteBuffer.getInt(0));
    }

    @Test
    void putThreeNumberValues() {
        // We test that the buffer automatically extends with more bytes to allow us to store multiple doubles
        bitBuffer.writeRawInt(1);
        bitBuffer.writeRawInt(2);
        bitBuffer.writeRawInt(3);

        ByteBuffer byteBuffer = bitBuffer.getFinishedByteBuffer();
        Assertions.assertEquals(12, byteBuffer.capacity());
        Assertions.assertEquals(1, byteBuffer.getInt(0));
        Assertions.assertEquals(2, byteBuffer.getInt(4));
        Assertions.assertEquals(3, byteBuffer.getInt(8));
    }

    @Test
    void writeBitString() {
        bitBuffer.writeBitString("00001111");
        ByteBuffer byteBuffer = bitBuffer.getFinishedByteBuffer();
        // We expect it to shorten the byte buffer down to 1 byte even though we allocated 2
        Assertions.assertEquals(1, byteBuffer.capacity());

        // We expect 0000 1111 which is equivalent to 15
        Assertions.assertEquals(15, byteBuffer.get(0));
    }

    @Test
    void writeBitStringWithUnfinishedByte() {
        // We have 9 bits i.e. an extra zero
        bitBuffer.writeBitString("111111110");
        ByteBuffer byteBuffer = bitBuffer.getFinishedByteBuffer();

        Assertions.assertEquals(2, byteBuffer.capacity());

        // We expect 1111 1111 which is equivalent to -1
        Assertions.assertEquals(-1, byteBuffer.get(0));
        // We expect it to fill it out with ones giving 0111 1111
        Assertions.assertEquals(127, byteBuffer.get(1));
    }

    @Test
    void writeLongBitString() {
        bitBuffer.writeBitString("000011111111000000000000");
        ByteBuffer byteBuffer = bitBuffer.getFinishedByteBuffer();
        // We expect it to automatically extend
        Assertions.assertEquals(3, byteBuffer.capacity());

        // We first expect 0000 1111 which is equivalent to 15
        Assertions.assertEquals(15, byteBuffer.get(0));
        // We then expect 11110000, which is equivalent to -16
        Assertions.assertEquals(-16, byteBuffer.get(1));
        // Then last we expect 00000000, which is equivalent to 0
        Assertions.assertEquals(0, byteBuffer.get(2));
    }

    @Test
    void writeTwoStrings() {
        bitBuffer.writeBitString("00001111");
        bitBuffer.writeBitString("11110000");
        ByteBuffer byteBuffer = bitBuffer.getFinishedByteBuffer();
        Assertions.assertEquals(2, byteBuffer.capacity());

        // We first expect 0000 1111 which is equivalent to 15
        Assertions.assertEquals(15, byteBuffer.get(0));
        // We then expect 11110000, which is equivalent to -16
        Assertions.assertEquals(-16, byteBuffer.get(1));
    }

}