package compression.utility;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class BitBufferTest {
    BitBuffer bitBuffer;

    @BeforeEach
    void beforeEach() {
        bitBuffer = new BitBuffer(2);
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
        bitBuffer.putInt(1);

        // We test that the buffer automatically extends to 4 bytes to store a float
        ByteBuffer byteBuffer = bitBuffer.getByteBuffer();
        Assertions.assertEquals(4, byteBuffer.capacity());
        Assertions.assertEquals(1, byteBuffer.getInt(0));
    }

    @Test
    void putThreeNumberValues() {
        // We test that the buffer automatically extends with more bytes to allow us to store multiple doubles
        bitBuffer.putInt(1);
        bitBuffer.putInt(2);
        bitBuffer.putInt(3);

        ByteBuffer byteBuffer = bitBuffer.getByteBuffer();
        Assertions.assertEquals(12, byteBuffer.capacity());
        Assertions.assertEquals(1, byteBuffer.getInt(0));
        Assertions.assertEquals(2, byteBuffer.getInt(4));
        Assertions.assertEquals(3, byteBuffer.getInt(8));
    }

    @Test
    void write1Bit() {
        bitBuffer.writeBit('1');

        ByteBuffer byteBuffer = bitBuffer.getByteBuffer();
        // We expect it to shorten the byte buffer down to 1 byte even though we allocated 2
        Assertions.assertEquals(1, byteBuffer.capacity());

        // We expect it to simply append the current byte with zeroes on the end of it
        // so we expect the following string "1000 0000", which is equivalent to - 128
        Assertions.assertEquals(-128, byteBuffer.get(0));
    }

    @Test
    void write8Bit() {
        bitBuffer.writeBit('1');
        bitBuffer.writeBit('1');
        bitBuffer.writeBit('1');
        bitBuffer.writeBit('1');
        bitBuffer.writeBit('1');
        bitBuffer.writeBit('1');
        bitBuffer.writeBit('1');
        bitBuffer.writeBit('1');

        ByteBuffer byteBuffer = bitBuffer.getByteBuffer();
        // We expect it to shorten the byte buffer down to 1 byte even though we allocated 2
        Assertions.assertEquals(1, byteBuffer.capacity());

        // We expect 1111 1111 which is equivalent to -1
        Assertions.assertEquals(-1, byteBuffer.get(0));
    }

    @Test
    void writeBitString() {
        bitBuffer.writeBitString("00001111");
        ByteBuffer byteBuffer = bitBuffer.getByteBuffer();
        // We expect it to shorten the byte buffer down to 1 byte even though we allocated 2
        Assertions.assertEquals(1, byteBuffer.capacity());

        // We expect 0000 1111 which is equivalent to 15
        Assertions.assertEquals(15, byteBuffer.get(0));
    }

    @Test
    void writeBitStringWithUnfinishedByte() {
        // We have 9 bits i.e. an extra zero
        bitBuffer.writeBitString("111111110");
        ByteBuffer byteBuffer = bitBuffer.getByteBuffer();

        Assertions.assertEquals(2, byteBuffer.capacity());

        // We expect 1111 1111 which is equivalent to -1
        Assertions.assertEquals(-1, byteBuffer.get(0));
        // We expect it to fill it out with zeroes giving 0000 0000
        Assertions.assertEquals(0, byteBuffer.get(1));
    }

    @Test
    void writeLongBitString() {
        bitBuffer.writeBitString("000011111111000000000000");
        ByteBuffer byteBuffer = bitBuffer.getByteBuffer();
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
        ByteBuffer byteBuffer = bitBuffer.getByteBuffer();
        Assertions.assertEquals(2, byteBuffer.capacity());

        // We first expect 0000 1111 which is equivalent to 15
        Assertions.assertEquals(15, byteBuffer.get(0));
        // We then expect 11110000, which is equivalent to -16
        Assertions.assertEquals(-16, byteBuffer.get(1));
    }

}