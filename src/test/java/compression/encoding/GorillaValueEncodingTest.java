package compression.encoding;

import compression.utility.BitBuffer;
import compression.utility.BitStream;
import compression.utility.BitUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GorillaValueEncodingTest {

    String removeSpace(String string) {
        return string.replace(" ", "");
    }

    @Test
    void encodeManyValues() {
        List<Float> values = List.of(1.0F, 1.0F, 1.0F, 2.0F, 4.0F, 7.0F);
        BitBuffer encoding =  GorillaValueEncoding.encode(values);
        BitStream bitStream = encoding.getBitStream();

        int integerRepresentationFirstValue = BitUtil.bits2Int(bitStream.getNBits(Integer.SIZE));
        float firstValue = Float.intBitsToFloat(integerRepresentationFirstValue);

        assertEquals(1.0F, firstValue);

        // We then expect two zero bits:
        assertEquals("0", bitStream.getNBits(1));
        assertEquals("0", bitStream.getNBits(1));


        // 1.0F xor 2.0F gives: 0111 1111 1000 0000 0000 0000 0000 0000
        // I.e. LZ = 1 and TZ = 23
        // So we expect:
        // CB (outside): 11
        // LZ (4-bits) : 1 -> 0001
        // L (5-bits)  : 8 -> 01000
        // SIGNIF-BITS : 1111 1111
        String expectedValue = removeSpace("11 0001 01000 1111 1111");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));

        // 2.0F xor 4.0F gives: 0000 0000 1000 0000 0000 0000 0000 0000
        // I.e. LZ = 8 and TZ = 23
        // So we expect:
        // CB (inside): 10 (as both values are greater than or equal to the previous ones)
        // SIGNIF-BITS : 0000 0001
        expectedValue = removeSpace("10 0000 0001");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));

        // 4.0F xor 7.0F gives: 0000 0000 0110 0000 0000 0000 0000 0000
        // I.e. LZ = 9 and TZ = 21
        // So we expect:
        // CB (outside): 11 (as for TZ, 21 < 23)
        // LZ (4-bits) : 9 -> 1001
        // L (5-bits)  : 2 -> 00010
        // SIGNIF-BITS : 11
        expectedValue = removeSpace("11 1001 00010 11");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));

        // As we have used 32+1+1+19+10+13 = 76 bits. So we allocated 10 bytes to fit these so we have 4 bits left.
        // We expect the buffer to fill these with ones.
        assertEquals("1111", bitStream.getNBits(4));

        // There should then be no bits left
        assertThrows(IndexOutOfBoundsException.class, () -> bitStream.getNBits(1));
    }

    @Test
    void encodeValueWithMoreLZThan15() {
        float v1 = Float.intBitsToFloat(0);
        float v2 = Float.intBitsToFloat(1);
        float v3 = Float.intBitsToFloat(0);

        List<Float> values = List.of(v1, v2, v3);
        BitBuffer encoding =  GorillaValueEncoding.encode(values);
        BitStream bitStream = encoding.getBitStream();

        int integerRepresentationFirstValue = BitUtil.bits2Int(bitStream.getNBits(Integer.SIZE));
        float firstValue = Float.intBitsToFloat(integerRepresentationFirstValue);

        assertEquals(v1, firstValue);

        // The xor of v1 and v2 should give: 0000 0000 0000 0000 0000 0000 0000 0001
        // I.e. LZ = 15 (as this is max) and L = 17
        // So we expect:
        // CB (outside): 11
        // LZ (4-bits) : 1 -> 1111
        // L (5-bits)  : 17 -> 10001
        // SIGNIF-BITS : 0000 0000 0000 0000 1
        String expectedValue = removeSpace("11 1111 10001 0000 0000 0000 0000 1");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));

        // The xor of v2 and v3 should give the same as above and therefore be inside the range
        // So we expect:
        // CB (inside): 10
        // SIGNIF-BITS : 0000 0000 0000 0000 1
        expectedValue = removeSpace("10 0000 0000 0000 0000 1");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));
    }

    @Test
    void encodeWhereLengthIs32() {
        float v1 = Float.intBitsToFloat(0);
        float v2 = Float.intBitsToFloat(-1);
        float v3 = Float.intBitsToFloat(0);

        List<Float> values = List.of(v1, v2, v3);
        BitBuffer encoding =  GorillaValueEncoding.encode(values);
        BitStream bitStream = encoding.getBitStream();

        int integerRepresentationFirstValue = BitUtil.bits2Int(bitStream.getNBits(Integer.SIZE));
        float firstValue = Float.intBitsToFloat(integerRepresentationFirstValue);

        assertEquals(v1, firstValue);

        // The xor of v1 and v2 should give: 1111 1111 1111 1111 1111 1111 1111 1111
        // I.e. LZ = 0 and L = 32
        // So we expect:
        // CB (outside): 11
        // LZ (4-bits) : 0 -> 0000
        // L (5-bits)  : 0 -> 00000 (as 32 is represented using 0)
        // SIGNIF-BITS : 1111 1111 1111 1111 1111 1111 1111 1111
        String expectedValue = removeSpace("11 0000 00000 1111 1111 1111 1111 1111 1111 1111 1111");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));

        // The xor of v2 and v3 should give the same as above and therefore be inside the range
        // So we expect:
        // CB (inside): 10
        // SIGNIF-BITS : 1111 1111 1111 1111 1111 1111 1111 1111
        expectedValue = removeSpace("10 1111 1111 1111 1111 1111 1111 1111 1111");
        assertEquals(expectedValue, bitStream.getNBits(expectedValue.length()));
    }

    @Test
    void decode() {
        List<Float> values = List.of(1.0F, 1.0F, 1.0F, 2.0F, 4.0F, 7.0F);
        BitBuffer encoding =  GorillaValueEncoding.encode(values);

        List<Float> decodedValues = GorillaValueEncoding.decode(encoding.getBitStream());
        assertEquals(values, decodedValues);
    }

    @Test
    void decode2() {
        List<Float> values = List.of(1.0F, 3.0F, 1.0F, 5.0F, 5.0F, 100000.0F);
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
}