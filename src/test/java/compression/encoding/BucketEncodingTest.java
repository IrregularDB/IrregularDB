package compression.encoding;

import compression.utility.BitBuffer;
import compression.utility.BitStream;
import compression.utility.BitUtil;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BucketEncodingTest {

    String removeSpace(String string) {
        return string.replace(" ", "");
    }

    BitStream getBitStreamForReadings(List<Integer> readings) {
        BitBuffer encoding = BucketEncoding.encode(readings);
        return encoding.getBitStream();
    }

    @Test
    void encodeValuesInBucket1() {
        // Bucket 1 uses the CB 01 and have a max size of 9
        int i1 = BitUtil.bits2Int(removeSpace("          1"));
        int i2 = BitUtil.bits2Int(removeSpace("       1111"));
        int i3 = BitUtil.bits2Int(removeSpace("1 0000 0000"));

        List<Integer> readings = List.of(i1, i2, i3);

        BitStream bitStream = getBitStreamForReadings(readings);
        // We expect the total size of the bit stream to be:
        //      - 3 * (2 + 9) = 33 bits
        //      - 7 bits to fill out the last byte
        assertEquals(40, bitStream.getSize());

        // We first expect the following pattern:
        // CB:      01
        // SIGNIF:  0 0000 0001
        String expectedValue = removeSpace("01 0 0000 0001");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));

        // We then expect
        // CB:      01
        // SIGNIF:  0 0000 1111
        expectedValue = removeSpace("01 0 0000 1111");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));

        // We then expect
        // CB:      01
        // SIGNIF:  1 0000 0000
        expectedValue = removeSpace("01 1 0000 0000");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));
    }

    @Test
    void encodeValuesInBucket2() {
        // Bucket 2 uses the CB 10 and have a size 10-16
        int i1 = BitUtil.bits2Int(removeSpace("       11 0000 0000"));
        int i2 = BitUtil.bits2Int(removeSpace("1111 1111 1111 1110"));

        List<Integer> readings = List.of(i1, i2);

        BitStream bitStream = getBitStreamForReadings(readings);
        // We expect the total size of the bit stream to be:
        //      - 2 * (2 + 16) = 36 bits
        //      - 4 bits to fill out the last byte
        assertEquals(40, bitStream.getSize());

        // We first expect the following pattern:
        // CB:      10
        // SIGNIF:  0000 0011 0000 0000
        String expectedValue = removeSpace("10 0000 0011 0000 0000");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));

        // We then expect
        // CB:      10
        // SIGNIF:  1111 1111 1111 1110
        expectedValue = removeSpace("10 1111 1111 1111 1110");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));
    }

    @Test
    void encodeValuesInBucket3() {
        // Bucket 2 uses the CB 11 and have a size 17-31
        int i1 = BitUtil.bits2Int(removeSpace("                 1 0000 0000 0000 0000"));
        int i2 = BitUtil.bits2Int(removeSpace("111 1111 1111 1111 1111 1111 1111 1111"));

        List<Integer> readings = List.of(i1, i2);

        BitStream bitStream = getBitStreamForReadings(readings);
        // We expect the total size of the bit stream to be:
        //      - 2 * (2 + 31) = 66 bits
        //      - 6 bits to fill out the last byte
        assertEquals(72, bitStream.getSize());

        // We first expect the following pattern:
        // CB:      11
        // SIGNIF:  000 0000 0000 0001 0000 0000 0000 0000
        String expectedValue = removeSpace("11 000 0000 0000 0001 0000 0000 0000 0000");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));

        // We then expect
        // CB:      11
        // SIGNIF:  111 1111 1111 1111 1111 1111 1111 1111
        expectedValue = removeSpace("11 111 1111 1111 1111 1111 1111 1111 1111");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));
    }

    @Test
    void encodeSameValues() {
        int i1 = BitUtil.bits2Int(removeSpace("1 0000 0000"));
        int i2 = BitUtil.bits2Int(removeSpace("1 0000 0000"));


        List<Integer> readings = List.of(i1, i2);

        BitStream bitStream = getBitStreamForReadings(readings);
        // We first use bucket 1 using (2+9 bits) then same value encoding using 2 bits
        // 11 + 2 = 13 (and then 3 bits filler) so 16 bits total
        assertEquals(16, bitStream.getSize());

        // We first expect the following pattern:
        // CB:      01
        // SIGNIF:  1 0000 0000
        String expectedValue = removeSpace("01 1 0000 0000");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));

        // We then expect the following:
        // CB: 00
        expectedValue = removeSpace("00");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));
    }

    @Test
    void encodeTooLargeValue() {
        // We try to insert a value with 32 bits, which we don't allow
        int i1 = BitUtil.bits2Int(removeSpace("1000 0000 0000 0000 0000 0000 0000 0000"));

        List<Integer> readings = List.of(i1);

        assertThrows(IllegalArgumentException.class, () -> BucketEncoding.encode(readings));
    }

    @Test
    void decode() {
    }
}