package compression.utility;

import compression.utility.BitBuffer.BitBuffer;
import compression.utility.BitBuffer.BitBufferNew;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utility.BitUtil;

import java.nio.ByteBuffer;

class BitBufferNewTest {
    BitBuffer bitBuffer;

    private String removeSpace(String string) {
        return string.replace(" ", "");
    }

    @BeforeEach
    void beforeEach() {
        bitBuffer = new BitBufferNew(true);
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
        Assertions.assertEquals(0b01111111, byteBuffer.get(0));
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