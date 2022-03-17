package compression.encoding;

import compression.timestamp.DeltaDeltaTimeStampCompression;
import compression.utility.BitBuffer.BitBuffer;
import compression.utility.BitStream.BitStream;
import org.junit.Before;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utility.BitPattern;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SignedBucketEncoderTest {

    private SignedBucketEncoder signedBucketEncoder;

    @BeforeEach
    void beforeEach(){
         signedBucketEncoder = new SignedBucketEncoder();
    }

    @Test
    void testSignedBucketEncoderEncodeNegativeValue(){
        int valueToEncode = -8;
        BitBuffer bitBuffer = signedBucketEncoder.encode(List.of(valueToEncode));
        BitStream bitStream = bitBuffer.getBitStream();

        BitPattern expectedBitPattern = new BitPattern("0 01 0 0000 1000");

        Assertions.assertEquals(expectedBitPattern.getIntRepresentation(),
                bitStream.getNextNBitsAsInteger(expectedBitPattern.getAmtBits()));
    }

    @Test
    void testSignedBucketEncoderEncodePositiveValue(){
        int valueToEncode = 8;
        BitBuffer bitBuffer = signedBucketEncoder.encode(List.of(valueToEncode));
        BitStream bitStream = bitBuffer.getBitStream();

        BitPattern expectedBitPattern = new BitPattern("1 01 0 0000 1000");

        Assertions.assertEquals(expectedBitPattern.getIntRepresentation(),
                bitStream.getNextNBitsAsInteger(expectedBitPattern.getAmtBits()));
    }

    @Test
    void testSignedBucketEncoderDecodeNegative(){
        int valueToEncode = -8;
        BitBuffer bitBuffer = signedBucketEncoder.encode(List.of(valueToEncode));

        Integer decompressedInteger = signedBucketEncoder.decodeSigned(bitBuffer.getBitStream()).get(0);

        Assertions.assertEquals(valueToEncode, decompressedInteger);
    }

    @Test
    void testSignedBucketEncoderDecodePositive(){
        int valueToEncode = 8;
        BitBuffer bitBuffer = signedBucketEncoder.encode(List.of(valueToEncode));

        Integer decompressedInteger = signedBucketEncoder.decodeSigned(bitBuffer.getBitStream()).get(0);

        Assertions.assertEquals(valueToEncode, decompressedInteger);
    }

    @Test
    void testSignedBucketEncoderThrowsExceptionIntMin(){
        Assertions.assertThrows(RuntimeException.class, () -> signedBucketEncoder.encode(List.of(Integer.MIN_VALUE)));
    }

    @Test
    void testSignedBucketEncoderEncodeNegativeValues(){
        List<Integer> valueToEncode = new ArrayList<>(List.of(-1, -100, -1000, -10000, -100000));
        BitBuffer bitBuffer = signedBucketEncoder.encode(valueToEncode);
        BitStream bitStream = bitBuffer.getBitStream();

        List<BitPattern> expectedBitPatterns = new ArrayList<>();
        expectedBitPatterns.add(new BitPattern("0 01 0 0000 0001"));
        expectedBitPatterns.add(new BitPattern("0 01 0 0110 0100"));
        expectedBitPatterns.add(new BitPattern("0 10 0000 0011 1110 1000"));
        expectedBitPatterns.add(new BitPattern("0 10 0010 0111 0001 0000"));
        expectedBitPatterns.add(new BitPattern("0 11"));
        expectedBitPatterns.add(new BitPattern("000 0000 0000 0001 1000 0110 1010 0000"));

        for (BitPattern expectedPattern : expectedBitPatterns){
            Assertions.assertEquals(expectedPattern.getIntRepresentation(),
                    bitStream.getNextNBitsAsInteger(expectedPattern.getAmtBits()));
        }
    }

    @Test
    void testSignedBucketEncoderEncodePositiveValues(){
        List<Integer> valueToEncode = new ArrayList<>(List.of(1, 100, 1000, 10000, 100000));
        BitBuffer bitBuffer = signedBucketEncoder.encode(valueToEncode);
        BitStream bitStream = bitBuffer.getBitStream();

        List<BitPattern> expectedBitPatterns = new ArrayList<>();
        expectedBitPatterns.add(new BitPattern("1 01 0 0000 0001"));
        expectedBitPatterns.add(new BitPattern("1 01 0 0110 0100"));
        expectedBitPatterns.add(new BitPattern("1 10 0000 0011 1110 1000"));
        expectedBitPatterns.add(new BitPattern("1 10 0010 0111 0001 0000"));
        expectedBitPatterns.add(new BitPattern("1 11"));
        expectedBitPatterns.add(new BitPattern("000 0000 0000 0001 1000 0110 1010 0000"));

        for (BitPattern expectedPattern : expectedBitPatterns){
            Assertions.assertEquals(expectedPattern.getIntRepresentation(),
                    bitStream.getNextNBitsAsInteger(expectedPattern.getAmtBits()));
        }
    }


    @Test
    void testSignedBucketEncoderEncodeNegativeAndPositiveValues(){
        List<Integer> valueToEncode = new ArrayList<>(List.of(1, -100, 1000, -10000, 100000));
        BitBuffer bitBuffer = signedBucketEncoder.encode(valueToEncode);
        BitStream bitStream = bitBuffer.getBitStream();

        List<BitPattern> expectedBitPatterns = new ArrayList<>();
        expectedBitPatterns.add(new BitPattern("1 01 0 0000 0001"));
        expectedBitPatterns.add(new BitPattern("0 01 0 0110 0100"));
        expectedBitPatterns.add(new BitPattern("1 10 0000 0011 1110 1000"));
        expectedBitPatterns.add(new BitPattern("0 10 0010 0111 0001 0000"));
        expectedBitPatterns.add(new BitPattern("1 11"));
        expectedBitPatterns.add(new BitPattern("000 0000 0000 0001 1000 0110 1010 0000"));

        for (BitPattern expectedPattern : expectedBitPatterns){
            Assertions.assertEquals(expectedPattern.getIntRepresentation(),
                    bitStream.getNextNBitsAsInteger(expectedPattern.getAmtBits()));
        }
    }

}