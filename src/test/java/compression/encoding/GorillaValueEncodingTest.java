package compression.encoding;

import compression.utility.BitBuffer.BitBuffer;
import compression.utility.BitStream.BitStream;
import utility.BitUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GorillaValueEncodingTest {

    String removeSpace(String string) {
        return string.replace(" ", "");
    }

    @Test
    void encodeManyFloatValues() {
        List<Float> values = List.of(1.0F, 1.0F, 1.0F, 2.0F, 4.0F, 7.0F);
        BitBuffer encoding =  GorillaValueEncoding.encode(values);
        BitStream bitStream = encoding.getBitStream();

        int integerRepresentationFirstValue = bitStream.getNextNBitsAsInteger(Integer.SIZE);
        float firstValue = Float.intBitsToFloat(integerRepresentationFirstValue);

        assertEquals(1.0F, firstValue);

        // We then expect two zero bits:
        assertEquals(0, bitStream.getNextNBitsAsInteger(1));
        assertEquals(0, bitStream.getNextNBitsAsInteger(1));


        // 1.0F xor 2.0F gives: 0111 1111 1000 0000 0000 0000 0000 0000
        // I.e. LZ = 1 and TZ = 23
        // So we expect:
        // CB (outside): 11
        // LZ (4-bits) : 1 -> 0001
        // L (5-bits)  : 8 -> 01000
        // SIGNIF-BITS : 1111 1111
        String expectedBitPattern = removeSpace("11 0001 01000 1111 1111");
        int expectedValue = BitUtil.bits2Int(expectedBitPattern);
        assertEquals(expectedValue, bitStream.getNextNBitsAsInteger(expectedBitPattern.length()));

        // 2.0F xor 4.0F gives: 0000 0000 1000 0000 0000 0000 0000 0000
        // I.e. LZ = 8 and TZ = 23
        // So we expect:
        // CB (inside): 10 (as both values are greater than or equal to the previous ones)
        // SIGNIF-BITS : 0000 0001
        expectedBitPattern = removeSpace("10 0000 0001");
        expectedValue = BitUtil.bits2Int(expectedBitPattern);
        assertEquals(expectedValue, bitStream.getNextNBitsAsInteger(expectedBitPattern.length()));

        // 4.0F xor 7.0F gives: 0000 0000 0110 0000 0000 0000 0000 0000
        // I.e. LZ = 9 and TZ = 21
        // So we expect:
        // CB (outside): 11 (as for TZ, 21 < 23)
        // LZ (4-bits) : 9 -> 1001
        // L (5-bits)  : 2 -> 00010
        // SIGNIF-BITS : 11
        expectedBitPattern = removeSpace("11 1001 00010 11");
        expectedValue = BitUtil.bits2Int(expectedBitPattern);
        assertEquals(expectedValue, bitStream.getNextNBitsAsInteger(expectedBitPattern.length()));

        // As we have used 32+1+1+19+10+13 = 76 bits. So we allocated 10 bytes to fit these so we have 4 bits left.
        // We expect the buffer to fill these with ones.
        expectedBitPattern = removeSpace("1111");
        expectedValue = BitUtil.bits2Int(expectedBitPattern);
        assertEquals(expectedValue, bitStream.getNextNBitsAsInteger(expectedBitPattern.length()));

        // There should then be no bits left
        assertThrows(IndexOutOfBoundsException.class, () -> bitStream.getNextNBitsAsInteger(1));
    }

    @Test
    void encodeValueWithMoreLZThan15() {
        float v1 = Float.intBitsToFloat(0);
        float v2 = Float.intBitsToFloat(1);
        float v3 = Float.intBitsToFloat(0);

        List<Float> values = List.of(v1, v2, v3);
        BitBuffer encoding =  GorillaValueEncoding.encode(values);
        BitStream bitStream = encoding.getBitStream();

        int integerRepresentationFirstValue = bitStream.getNextNBitsAsInteger(Integer.SIZE);
        float firstValue = Float.intBitsToFloat(integerRepresentationFirstValue);

        assertEquals(v1, firstValue);

        // The xor of v1 and v2 should give: 0000 0000 0000 0000 0000 0000 0000 0001
        // I.e. LZ = 15 (as this is max) and L = 17
        // So we expect:
        // CB (outside): 11
        // LZ (4-bits) : 1 -> 1111
        // L (5-bits)  : 17 -> 10001
        // SIGNIF-BITS : 0000 0000 0000 0000 1
        String expectedBitPattern = removeSpace("11 1111 10001 0000 0000 0000 0000 1");
        int expectedValue = BitUtil.bits2Int(expectedBitPattern);
        assertEquals(expectedValue, bitStream.getNextNBitsAsInteger(expectedBitPattern.length()));

        // The xor of v2 and v3 should give the same as above and therefore be inside the range
        // So we expect:
        // CB (inside): 10
        // SIGNIF-BITS : 0000 0000 0000 0000 1
        expectedBitPattern = removeSpace("10 0000 0000 0000 0000 1");
        expectedValue = BitUtil.bits2Int(expectedBitPattern);
        assertEquals(expectedValue, bitStream.getNextNBitsAsInteger(expectedBitPattern.length()));
    }


    // NOTICE: we had to split this assertion in this test into two as their patterns are longer than 32 bits
    @Test
    void encodeWhereLengthIs32() {
        float v1 = Float.intBitsToFloat(0);
        float v2 = Float.intBitsToFloat(-1);
        float v3 = Float.intBitsToFloat(0);

        List<Float> values = List.of(v1, v2, v3);
        BitBuffer encoding =  GorillaValueEncoding.encode(values);
        BitStream bitStream = encoding.getBitStream();

        int integerRepresentationFirstValue = bitStream.getNextNBitsAsInteger(Integer.SIZE);
        float firstValue = Float.intBitsToFloat(integerRepresentationFirstValue);

        assertEquals(v1, firstValue);

        // The xor of v1 and v2 should give: 1111 1111 1111 1111 1111 1111 1111 1111
        // I.e. LZ = 0 and L = 32
        // So we expect:
        // CB (outside): 11
        // LZ (4-bits) : 0 -> 0000
        // L (5-bits)  : 0 -> 00000 (as 32 is represented using 0)
        // SIGNIF-BITS : 1111 1111 1111 1111 1111 1111 1111 1111
        String expectedBitPattern = removeSpace("11 0000 00000");
        int expectedValue = BitUtil.bits2Int(expectedBitPattern);
        assertEquals(expectedValue, bitStream.getNextNBitsAsInteger(expectedBitPattern.length()));

        expectedBitPattern = removeSpace("1111 1111 1111 1111 1111 1111 1111 1111");
        expectedValue = BitUtil.bits2Int(expectedBitPattern);
        assertEquals(expectedValue, bitStream.getNextNBitsAsInteger(expectedBitPattern.length()));

        // The xor of v2 and v3 should give the same as above and therefore be inside the range
        // So we expect:
        // CB (inside): 10
        // SIGNIF-BITS : 1111 1111 1111 1111 1111 1111 1111 1111
        expectedBitPattern = removeSpace("10");
        expectedValue = BitUtil.bits2Int(expectedBitPattern);
        assertEquals(expectedValue, bitStream.getNextNBitsAsInteger(expectedBitPattern.length()));

        expectedBitPattern = removeSpace("1111 1111 1111 1111 1111 1111 1111 1111");
        expectedValue = BitUtil.bits2Int(expectedBitPattern);
        assertEquals(expectedValue, bitStream.getNextNBitsAsInteger(expectedBitPattern.length()));
    }

    @Test
    void decodeManyFloatValues() {
        List<Float> values = List.of(1.0F, 1.0F, 1.0F, 2.0F, 4.0F, 7.0F);
        BitBuffer encoding =  GorillaValueEncoding.encode(values);

        List<Float> decodedValues = GorillaValueEncoding.decode(encoding.getBitStream());
        assertEquals(values, decodedValues);
    }

    @Test
    void decodeValueWithMoreLZThan15() {
        float v1 = Float.intBitsToFloat(0);
        float v2 = Float.intBitsToFloat(1);
        float v3 = Float.intBitsToFloat(0);

        List<Float> values = List.of(v1, v2, v3);
        BitBuffer encoding =  GorillaValueEncoding.encode(values);

        List<Float> decodedValues = GorillaValueEncoding.decode(encoding.getBitStream());
        assertEquals(values, decodedValues);
    }

    @Test
    void decodeWhereLengthIs32() {
        float v1 = Float.intBitsToFloat(0);
        float v2 = Float.intBitsToFloat(-1);
        float v3 = Float.intBitsToFloat(0);

        List<Float> values = List.of(v1, v2, v3);
        BitBuffer encoding =  GorillaValueEncoding.encode(values);
        List<Float> decodedValues = GorillaValueEncoding.decode(encoding.getBitStream());
        assertEquals(values, decodedValues);
    }

    @Test
    void decodeWithOneEmptyBit() {
        float v1 = Float.intBitsToFloat(0);
        List<Float> values = new ArrayList<>();
        int amtValuesToInsert = 8;
        for (int i = 0; i < amtValuesToInsert; i++) {
            values.add(v1);
        }
        BitBuffer encoding =  GorillaValueEncoding.encode(values);
        BitStream bitStream = encoding.getBitStream();
        // We added the same value 8 times. We therefore expect it to encode this as 40 bits:
            // - first 32 bits for the original value
            // - then 7 * times 1 bit used to store control bit "0"
            // - then padding one "1"'s on the end
        Assertions.assertEquals(40, bitStream.getSize());

        // The decompressor should ignore the last padding bit
        List<Float> decodedValues = GorillaValueEncoding.decode(bitStream);
        assertEquals(values, decodedValues);
    }

    @Test
    void decodeWithMultipleEmptyBits() {
        float v1 = Float.intBitsToFloat(0);
        List<Float> values = new ArrayList<>();
        int amtValuesToInsert = 2;
        for (int i = 0; i < amtValuesToInsert; i++) {
            values.add(v1);
        }
        BitBuffer encoding =  GorillaValueEncoding.encode(values);
        BitStream bitStream = encoding.getBitStream();
        // We added the same value 8 times. We therefore expect it to encode this as 40 bits:
        // - first 32 bits for the original value
        // - then 1 * times 1 bit used to store control bit "0"
        // - then padding 7 "1"'s on the end
        Assertions.assertEquals(40, bitStream.getSize());

        // The decompressor should ignore the last padding bits
        List<Float> decodedValues = GorillaValueEncoding.decode(bitStream);
        assertEquals(values, decodedValues);
    }

    // This test checks we handle the edge case where we hit 101 as last bits
    @Test
    void decodeWhereWeHit101AsLastBits() {
        float v1 = Float.intBitsToFloat(0);
        float v2 = Float.intBitsToFloat(Integer.MIN_VALUE);

        List<Float> values = List.of(v1, v2, v2, v1);

        BitBuffer encoding =  GorillaValueEncoding.encode(values);

        BitStream bitStream = encoding.getBitStream();
        /* We here expect it to use
            - first 32 bits for the original value
            - Then for the next value we get 12 bits: as XOR = 1000 0000 ...
                - CB = 11
                - LZ = 0000
                - L  = 00001
                - SIGNIF-BITS = 1
            - Then 0 as we store the same value
            - Then 101 because XOR is again XOR = 1000 0000 ... which is within the previous range and SIGNIF-BITS length is 1
        */
        Assertions.assertEquals(48, bitStream.getSize());

        List<Float> decodedValues = GorillaValueEncoding.decode(bitStream);
        assertEquals(values, decodedValues);
    }

}