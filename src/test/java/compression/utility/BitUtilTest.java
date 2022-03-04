package compression.utility;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


class BitUtilTest {

    @Test
    public void testInt2Bits(){
        int testInt = 7;

        Assertions.assertEquals("111", BitUtil.int2Bits(testInt));

        testInt = 255;
        Assertions.assertEquals("11111111", BitUtil.int2Bits(testInt));
    }

    @Test
    public void testByte2Bits() {
        byte testByte = 34; // 10 0010 should become 0010 0010 (as we pad with 2 zeroes)
        Assertions.assertEquals("00100010", BitUtil.byte2Bits(testByte));
    }

    @Test
    public void testBytesAreViewedAsUnsigned() {
        byte testByte = -1;
        String byteString = BitUtil.byte2Bits(testByte);

        // This might seem a bit weird but it is because we look at the bit pattern of the 8 bits of the byte
        Assertions.assertEquals(255, BitUtil.bits2Int(byteString));
    }

    @Test
    public void testByte2BitsNegativeNumbers() {
        byte testByte = -1; // The bit-pattern should be 1111 1111
        Assertions.assertEquals("11111111", BitUtil.byte2Bits(testByte));

        testByte = -128; // The bit-pattern should be 1000 0000
        Assertions.assertEquals("10000000", BitUtil.byte2Bits(testByte));

        testByte = -10;
        Assertions.assertEquals("11110110", BitUtil.byte2Bits(testByte));
    }

}