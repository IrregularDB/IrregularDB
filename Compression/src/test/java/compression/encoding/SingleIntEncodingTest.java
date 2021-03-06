package compression.encoding;

import compression.utility.BitStream.BitStream;
import compression.utility.BitStream.BitStreamNew;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import utility.BitPattern;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SingleIntEncodingTest {

    @Test
    void encodeIntWithLength9() {
        // We encode an integer with length 9, so we expect it to use two bytes and pad 7 zeroes in front.
        int integer = new BitPattern("1 1111 1111").getIntRepresentation();

        BitStream bitStream = new BitStreamNew(SingleIntEncoding.encode(integer));

        BitPattern expectedBitPattern = new BitPattern("0000 0001 1111 1111");

        Assertions.assertEquals(expectedBitPattern.getIntRepresentation(), bitStream.getNextNBitsAsInteger(bitStream.getSize()));
    }

    @Test
    void decode() {
        int integer = 100;

        ByteBuffer encoding = SingleIntEncoding.encode(integer);

        Assertions.assertEquals(integer, SingleIntEncoding.decode(encoding));
    }
}