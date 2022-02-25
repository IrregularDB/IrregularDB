package compression.encoding;

import compression.utility.BitBuffer;
import compression.utility.BitStream;
import compression.utility.BitUtil;

import java.util.List;

public class GorillaValueEncoding {
    private static final String SAME_VALUE_CONTROL_BIT = "0";
    private static final String INSIDE_RANGE_CONTROL_BIT = "10";
    private static final String OUTSIDE_RANGE_CONTROL_BIT = "11";

    // Notice these values are one smaller than in standard GORILLA as we work on floats
    private static final int AMT_BITS_USED_FOR_LEADING_ZEROES = 4;
    private static final int AMT_BITS_USED_FOR_LENGTH = 5;


    public static BitBuffer encode(List<Float> values) {
        BitBuffer bitBuffer = new BitBuffer(16);
        int previousLeadingZeroes = -1;
        int previousTrailingZeroes = -1;

        // Get BIT representation of the float value
        int previousValue =  Float.floatToIntBits(values.get(0));

        for (int i = 1; i < values.size(); i++) {
            var currValue = Float.floatToIntBits(values.get(i));
            int xor = currValue ^ previousValue;
            if (xor == 0) {
                bitBuffer.writeBitString(SAME_VALUE_CONTROL_BIT);
            } else {
                int leadingZeroes = Integer.numberOfLeadingZeros(currValue);
                int trailingZeroes = Integer.numberOfTrailingZeros(currValue);

                // When there is less leading zeroes or trailing zeroes then we have to store a new range
                if ((previousLeadingZeroes == -1) || (leadingZeroes < previousLeadingZeroes) || (trailingZeroes < previousTrailingZeroes)) {
                    bitBuffer.writeBitString(createOutSideRangeString(leadingZeroes, trailingZeroes, currValue));
                    previousLeadingZeroes = leadingZeroes;
                    previousTrailingZeroes = trailingZeroes;
                } else {
                    int lengthOfSignificantBits = Integer.SIZE - previousLeadingZeroes - previousTrailingZeroes;

                }
            }
        }

        return bitBuffer;
    }

    private static String createOutSideRangeString(int leadingZeroes, int trailingZeroes, int currValue) {
        int lengthOfSignificantBits = Integer.SIZE - leadingZeroes - trailingZeroes - 1; // The minus 1 is necessary to store length = 32
        return OUTSIDE_RANGE_CONTROL_BIT +
                createBitEncodingWithSpecifiedLength(leadingZeroes, AMT_BITS_USED_FOR_LEADING_ZEROES) +   // LZ
                createBitEncodingWithSpecifiedLength(lengthOfSignificantBits, AMT_BITS_USED_FOR_LENGTH) + // L
                createSignificantBitsString(currValue, lengthOfSignificantBits);                          // Signif-bits
    }

    private static String createBitEncodingWithSpecifiedLength(int value, int amtBits) {
        String bitString = BitUtil.int2Bits(value);
        int amtBitsInValue = bitString.length();
        int zeroesToPad = amtBits - amtBitsInValue;

        return "0".repeat(zeroesToPad) + bitString;
    }

    private static String createSignificantBitsString(int value, int length) {
        throw new RuntimeException("NOT IMPLEMENTED");
    }

    public static List<Float> decode(BitStream bitStream) {
        throw new RuntimeException("Not implemented");
    }
}
