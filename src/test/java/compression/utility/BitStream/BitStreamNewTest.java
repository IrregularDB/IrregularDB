package compression.utility.BitStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utility.BitUtil;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.*;

class BitStreamNewTest {
    BitStream bitStream;

    @BeforeEach
    void beforeEach() { // Setup bit stream with 1111 1111 0000 0000
        byte b = -1;
        ByteBuffer byteBuffer = ByteBuffer.allocate(2);
        byteBuffer.put(b); // We put 1111 1111
        b = 0;
        byteBuffer.put(b); // We put 0000 0000
        bitStream = new BitStreamNew(byteBuffer);
    }


    @Test
    void getNBits() {
        int actualNbits = bitStream.getNextNBitsAsInteger(12);

        assertEquals(BitUtil.bits2Int("111111110000"), actualNbits);
    }

    @Test
    void getNBitsTwice() {
        int actualNbits = bitStream.getNextNBitsAsInteger(7);

        assertEquals(BitUtil.bits2Int("1111111"), actualNbits);

        actualNbits = bitStream.getNextNBitsAsInteger(9);

        assertEquals(BitUtil.bits2Int("100000000"), actualNbits);
    }

    @Test
    void getAll16Bits() {
        int actualNbits = bitStream.getNextNBitsAsInteger(16);

        assertEquals(BitUtil.bits2Int("1111111100000000"), actualNbits);
    }

    @Test
    void getTooManyBits() {
        assertThrows(IndexOutOfBoundsException.class, () -> bitStream.getNextNBitsAsInteger(17));
    }

    @Test
    void getIllegalAmountBits() {
        assertThrows(IllegalArgumentException.class, () -> bitStream.getNextNBitsAsInteger(0));
    }

    @Test
    void hasNNext() {
        // We expect it to have 16 next before anything has been done to it:
        Assertions.assertTrue(bitStream.hasNNext(16));
        Assertions.assertFalse(bitStream.hasNNext(17));
    }

    @Test
    void hasNNextAfterRemovingElements() {
        bitStream.getNextNBitsAsInteger(4);

        // We expect it to no longer have 16 bits:
        Assertions.assertFalse(bitStream.hasNNext(16));
        // We instead expect it to have 12 after removing the first 4 bits
        Assertions.assertTrue(bitStream.hasNNext(12));
    }
}