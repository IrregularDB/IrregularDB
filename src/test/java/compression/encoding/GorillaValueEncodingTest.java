package compression.encoding;

import compression.utility.BitBuffer;
import compression.utility.BitStream;
import compression.utility.BitUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class GorillaValueEncodingTest {
    List<Float> values;
    BitBuffer encoding;
    BitStream bitStream;

    @BeforeEach
    void beforeEach() {
        values = List.of(1.0F, 1.0F, 1.0F, 2.0F, 4.0F, 7.0F);
        encoding = GorillaValueEncoding.encode(values);
    }


    @Test
    void encode() {
        bitStream = encoding.getBitStream();
        int integerRepresentationFirstValue = BitUtil.bits2Int(bitStream.getNBits(32));
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
        assertEquals("1100010100011111111", bitStream.getNBits(19));

        // 2.0F xor 4.0F gives: 0000 0000 1000 0000 0000 0000 0000 0000
        // I.e. LZ = 8 and TZ = 23
        // So we expect:
        // CB (inside): 10 (as both values are greater than or equal to the previous ones)
        // SIGNIF-BITS : 0000 0001
        assertEquals("1000000001", bitStream.getNBits(10));

        // 4.0F xor 7.0F gives: 0000 0000 0110 0000 0000 0000 0000 0000
        // I.e. LZ = 9 and TZ = 21
        // So we expect:
        // CB (outside): 11 (as for TZ, 21 < 23)
        // LZ (4-bits) : 9 -> 1001
        // L (5-bits)  : 2 -> 00010
        // SIGNIF-BITS : 11
        assertEquals("1110010001011", bitStream.getNBits(13));

        // As we have used 32+1+1+19+10+13 = 76 bits. So we allocated 10 bytes to fit these so we have 4 bits left.
        // We expect the buffer to fill these with ones.
        assertEquals("1111", bitStream.getNBits(4));

        // There should then be no bits left
        assertThrows(IndexOutOfBoundsException.class, () -> bitStream.getNBits(1));
    }

    @Test
    void decode() {
        List<Float> decodedValues = GorillaValueEncoding.decode(encoding.getBitStream());

        assertEquals(values, decodedValues);
    }
}