package compression.encoding;

import compression.utility.BitBuffer.BitBuffer;
import compression.utility.BitStream.BitStream;
import compression.utility.BitStream.BitStreamNew;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utility.BitPattern;

import java.util.ArrayList;
import java.util.List;

class SignedBucketEncoderTest {

    private BucketEncoding signedBucketEncoder;

    @BeforeEach
    void beforeEach(){
         signedBucketEncoder = new BucketEncoding(true);
    }

    @Test
    void testSignedBucketEncoderEncodeNegativeValue(){
        int valueToEncode = -8;
        signedBucketEncoder.encode(List.of(valueToEncode));
        BitStream bitStream = new BitStreamNew(signedBucketEncoder.getByteBuffer());

        BitPattern expectedBitPattern = new BitPattern("01 0 0 0000 1000");

        Assertions.assertEquals(expectedBitPattern.getIntRepresentation(),
                bitStream.getNextNBitsAsInteger(expectedBitPattern.getAmtBits()));
    }

    @Test
    void testSignedBucketEncoderEncodePositiveValue(){
        int valueToEncode = 8;
        signedBucketEncoder.encode(List.of(valueToEncode));
        BitStream bitStream = new BitStreamNew(signedBucketEncoder.getByteBuffer());

        BitPattern expectedBitPattern = new BitPattern("01 1 0 0000 1000");

        Assertions.assertEquals(expectedBitPattern.getIntRepresentation(),
                bitStream.getNextNBitsAsInteger(expectedBitPattern.getAmtBits()));
    }

    @Test
    void testSignedBucketEncoderDecodeNegative(){
        int valueToEncode = -8;
        signedBucketEncoder.encode(List.of(valueToEncode));
        BitStream bitStream = new BitStreamNew(signedBucketEncoder.getByteBuffer());

        Integer decompressedInteger = BucketEncoding.decode(bitStream, true).get(0);

        Assertions.assertEquals(valueToEncode, decompressedInteger);
    }

    @Test
    void testSignedBucketEncoderDecodePositive(){
        int valueToEncode = 8;
        signedBucketEncoder.encode(List.of(valueToEncode));
        BitStream bitStream = new BitStreamNew(signedBucketEncoder.getByteBuffer());

        Integer decompressedInteger = BucketEncoding.decode(bitStream, true).get(0);

        Assertions.assertEquals(valueToEncode, decompressedInteger);
    }

    @Test
    void testSignedBucketEncoderThrowsExceptionIntMin(){
        Assertions.assertThrows(RuntimeException.class, () -> signedBucketEncoder.encode(List.of(Integer.MIN_VALUE)));
    }

    @Test
    void testSignedBucketEncoderEncodeNegativeValues(){
        List<Integer> valuesToEncode = new ArrayList<>(List.of(-1, -100, -1000, -10000, -100000));
        signedBucketEncoder.encode(valuesToEncode);
        BitStream bitStream = new BitStreamNew(signedBucketEncoder.getByteBuffer());

        List<BitPattern> expectedBitPatterns = new ArrayList<>();
        expectedBitPatterns.add(new BitPattern("01 0 0 0000 0001"));
        expectedBitPatterns.add(new BitPattern("01 0 0 0110 0100"));
        expectedBitPatterns.add(new BitPattern("10 0 0000 0011 1110 1000"));
        expectedBitPatterns.add(new BitPattern("10 0 0010 0111 0001 0000"));
        //The below two rows represent a single value, the split is necessary as this uses 34 bits.
        expectedBitPatterns.add(new BitPattern("11 0"));
        expectedBitPatterns.add(new BitPattern("000 0000 0000 0001 1000 0110 1010 0000"));

        for (BitPattern expectedPattern : expectedBitPatterns){
            Assertions.assertEquals(expectedPattern.getIntRepresentation(),
                    bitStream.getNextNBitsAsInteger(expectedPattern.getAmtBits()));
        }
    }

    @Test
    void testSignedBucketEncoderEncodePositiveValues(){
        List<Integer> valuesToEncode = new ArrayList<>(List.of(1, 100, 1000, 10000, 100000));
        signedBucketEncoder.encode(valuesToEncode);
        BitStream bitStream = new BitStreamNew(signedBucketEncoder.getByteBuffer());

        List<BitPattern> expectedBitPatterns = new ArrayList<>();
        expectedBitPatterns.add(new BitPattern("01 1 0 0000 0001"));
        expectedBitPatterns.add(new BitPattern("01 1 0 0110 0100"));
        expectedBitPatterns.add(new BitPattern("10 1 0000 0011 1110 1000"));
        expectedBitPatterns.add(new BitPattern("10 1 0010 0111 0001 0000"));
        //The below two rows represent a single value, the split is necessary as this uses 34 bits.
        expectedBitPatterns.add(new BitPattern("11 1"));
        expectedBitPatterns.add(new BitPattern("000 0000 0000 0001 1000 0110 1010 0000"));

        for (BitPattern expectedPattern : expectedBitPatterns){
            Assertions.assertEquals(expectedPattern.getIntRepresentation(),
                    bitStream.getNextNBitsAsInteger(expectedPattern.getAmtBits()));
        }
    }


    @Test
    void testSignedBucketEncoderEncodeNegativeAndPositiveValues(){
        List<Integer> valuesToEncode = new ArrayList<>(List.of(1, -100, 1000, -10000, 100000));
        signedBucketEncoder.encode(valuesToEncode);
        BitStream bitStream = new BitStreamNew(signedBucketEncoder.getByteBuffer());

        List<BitPattern> expectedBitPatterns = new ArrayList<>();
        expectedBitPatterns.add(new BitPattern("01 1 0 0000 0001"));
        expectedBitPatterns.add(new BitPattern("01 0 0 0110 0100"));
        expectedBitPatterns.add(new BitPattern("10 1 0000 0011 1110 1000"));
        expectedBitPatterns.add(new BitPattern("10 0 0010 0111 0001 0000"));
        //The below two rows represent a single value, the split is necessary as this uses 34 bits.
        expectedBitPatterns.add(new BitPattern("11 1"));
        expectedBitPatterns.add(new BitPattern("000 0000 0000 0001 1000 0110 1010 0000"));

        for (BitPattern expectedPattern : expectedBitPatterns){
            Assertions.assertEquals(expectedPattern.getIntRepresentation(),
                    bitStream.getNextNBitsAsInteger(expectedPattern.getAmtBits()));
        }
    }

    @Test
    void encodingTheSameValue(){
        List<Integer> valuesToEncode = new ArrayList<>(List.of(1, 1));
        signedBucketEncoder.encode(valuesToEncode);
        BitStream bitStream = new BitStreamNew(signedBucketEncoder.getByteBuffer());

        List<BitPattern> expectedBitPatterns = new ArrayList<>();
        expectedBitPatterns.add(new BitPattern("01 1 0 0000 0001"));
        expectedBitPatterns.add(new BitPattern("00"));

        for (BitPattern expectedPattern : expectedBitPatterns){
            Assertions.assertEquals(expectedPattern.getIntRepresentation(),
                    bitStream.getNextNBitsAsInteger(expectedPattern.getAmtBits()));
        }
    }

    @Test
    void encodingTheSameValueNegative(){
        List<Integer> valuesToEncode = new ArrayList<>(List.of(-1, -1));
        signedBucketEncoder.encode(valuesToEncode);
        BitStream bitStream = new BitStreamNew(signedBucketEncoder.getByteBuffer());

        List<BitPattern> expectedBitPatterns = new ArrayList<>();
        expectedBitPatterns.add(new BitPattern("01 0 0 0000 0001"));
        expectedBitPatterns.add(new BitPattern("00"));

        for (BitPattern expectedPattern : expectedBitPatterns){
            Assertions.assertEquals(expectedPattern.getIntRepresentation(),
                    bitStream.getNextNBitsAsInteger(expectedPattern.getAmtBits()));
        }
    }

    @Test
    void encodingTheSameAbsoluteValue(){
        List<Integer> valuesToEncode = new ArrayList<>(List.of(1, -1));
        signedBucketEncoder.encode(valuesToEncode);
        BitStream bitStream = new BitStreamNew(signedBucketEncoder.getByteBuffer());

        List<BitPattern> expectedBitPatterns = new ArrayList<>();
        expectedBitPatterns.add(new BitPattern("01 1 0 0000 0001"));
        // We don't see -1 and 1 as the same numbers so expect it to the entire value again
        expectedBitPatterns.add(new BitPattern("01 0 0 0000 0001"));

        for (BitPattern expectedPattern : expectedBitPatterns){
            Assertions.assertEquals(expectedPattern.getIntRepresentation(),
                    bitStream.getNextNBitsAsInteger(expectedPattern.getAmtBits()));
        }
    }

}