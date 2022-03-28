package compression.encoding;

import compression.utility.BitBuffer.BitBuffer;
import compression.utility.BitBuffer.BitBufferNew;
import compression.utility.BitStream.BitStream;

import java.util.ArrayList;
import java.util.List;

public class GorillaValueEncoding {
    private static final byte SAME_VALUE_CONTROL_BIT = 0b0;
    private static final byte INSIDE_RANGE_CONTROL_BIT = 0b10;
    private static final byte OUTSIDE_RANGE_CONTROL_BIT = 0b11;

    // Notice these values are one smaller than in standard GORILLA as we work on floats
    private static final int AMT_BITS_USED_FOR_LEADING_ZEROES = 4;
    private static final int AMT_BITS_USED_FOR_LENGTH = 5;


    public static BitBuffer encode(List<Float> values) {
        // We finish the byte with 1's as we can then in the decoding detect end of stream
        BitBuffer bitBuffer = new BitBufferNew(true);
        int previousLeadingZeroes = Integer.MAX_VALUE;
        int previousTrailingZeroes = Integer.MAX_VALUE;

        // Get BIT representation of the float value and write the initial value to the buffer
        int previousValue =  Float.floatToRawIntBits(values.get(0));
        bitBuffer.writeIntUsingNBits(previousValue, Integer.SIZE);

        for (int i = 1; i < values.size(); i++) {
            var currValue = Float.floatToRawIntBits(values.get(i));
            int xor = currValue ^ previousValue;
            if (xor == 0) {
                writeControlBitToBuffer(SAME_VALUE_CONTROL_BIT, bitBuffer);
            } else {
                int leadingZeroes = Integer.numberOfLeadingZeros(xor);
                if (leadingZeroes > 15) { // We only use 4 bits for LZ so we can at max represent 15
                    leadingZeroes = 15;
                }
                int trailingZeroes = Integer.numberOfTrailingZeros(xor);

                if ((leadingZeroes >= previousLeadingZeroes) && (trailingZeroes >= previousTrailingZeroes)) {
                    // We can reuse the old range when there is less than or same amount of leading zeroes and trailing
                    // zeroes in our current value
                    writeInsideRangeString(bitBuffer, previousLeadingZeroes, previousTrailingZeroes, xor);
                } else {
                    // When there is less leading zeroes or trailing zeroes then we have to store a new range
                    writeOutSideRangeString(bitBuffer, leadingZeroes, trailingZeroes, xor);
                    previousLeadingZeroes = leadingZeroes;
                    previousTrailingZeroes = trailingZeroes;
                }
            }
            previousValue = currValue;
        }

        return bitBuffer;
    }

    private static void writeControlBitToBuffer(byte controlBits, BitBuffer bitBuffer) {
        switch (controlBits) {
            case SAME_VALUE_CONTROL_BIT -> bitBuffer.writeFalseBit();
            case INSIDE_RANGE_CONTROL_BIT -> {
                bitBuffer.writeTrueBit();
                bitBuffer.writeFalseBit();
            }
            case OUTSIDE_RANGE_CONTROL_BIT -> {
                bitBuffer.writeTrueBit();
                bitBuffer.writeTrueBit();
            }
            default -> throw new IllegalArgumentException("unknown controlBits");
        }
    }

    private static void writeOutSideRangeString(BitBuffer bitBuffer, int leadingZeroes, int trailingZeroes, int currValue) {
        int lengthOfSignificantBits = Integer.SIZE - leadingZeroes - trailingZeroes ;
        if (lengthOfSignificantBits == 32) {  // We represent length 32 as 0 (because the case of zero would mean the values are the same XOR == 0 case instead
            lengthOfSignificantBits = 0;
        }

        writeControlBitToBuffer(OUTSIDE_RANGE_CONTROL_BIT, bitBuffer);
        bitBuffer.writeIntUsingNBits(leadingZeroes, AMT_BITS_USED_FOR_LEADING_ZEROES); // LZ
        bitBuffer.writeIntUsingNBits(lengthOfSignificantBits, AMT_BITS_USED_FOR_LENGTH); // L

        writeSignificantBitsToBuffer(currValue, trailingZeroes, lengthOfSignificantBits, bitBuffer); // SIGNIF-BITS
    }

    private static void writeSignificantBitsToBuffer(int value, int trailingZeroes, int lengthOfSignificantBits, BitBuffer bitBuffer) {
        if (lengthOfSignificantBits == 0) {
            lengthOfSignificantBits = 32;
        }
        // we use zero-fill right shifting
        int shiftedValue = value >>> trailingZeroes;
        bitBuffer.writeIntUsingNBits(shiftedValue, lengthOfSignificantBits);
    }

    private static void writeInsideRangeString(BitBuffer bitBuffer, int previousLeadingZeroes, int previousTrailingZeroes, int currValue) {
        int lengthOfSignificantBits = Integer.SIZE - previousLeadingZeroes - previousTrailingZeroes;

        writeControlBitToBuffer(INSIDE_RANGE_CONTROL_BIT, bitBuffer);
        writeSignificantBitsToBuffer(currValue, previousTrailingZeroes, lengthOfSignificantBits, bitBuffer); // SIGNIF-BITS
    }

    public static List<Float> decode(BitStream bitStream) {
        List<Float> floatValues = new ArrayList<>();

        int leadingZeroes = Integer.MAX_VALUE;
        int trailingZeroes = Integer.MAX_VALUE;
        int length = Integer.MAX_VALUE;


        int previousValue = bitStream.getNextNBitsAsInteger(Integer.SIZE);
        floatValues.add(Float.intBitsToFloat(previousValue));
        byte controlBit;
        while (bitStream.hasNNext(1)) {
            controlBit = (byte) bitStream.getNextNBitsAsInteger(1);
            if (controlBit == SAME_VALUE_CONTROL_BIT) {
                floatValues.add(Float.intBitsToFloat(previousValue));
            } else {
                if (!bitStream.hasNNext(2)){
                    break; //this indicates end of stream, and remaining bits of stream are without significance
                }
                controlBit = (byte) bitStream.getNextNBitsAsInteger(1);
                if (controlBit == 1) { // New leading zero and trailing as we had 11
                    if (!bitStream.hasNNext(AMT_BITS_USED_FOR_LEADING_ZEROES + AMT_BITS_USED_FOR_LENGTH)) {
                        break; //this indicates end of stream, and remaining bits of stream are without significance
                    }
                    // Calculate new values:
                    leadingZeroes = bitStream.getNextNBitsAsInteger(AMT_BITS_USED_FOR_LEADING_ZEROES);
                    length = bitStream.getNextNBitsAsInteger(AMT_BITS_USED_FOR_LENGTH);
                    if (length == 0) {
                        length = 32;
                    }
                    trailingZeroes = Integer.SIZE - length - leadingZeroes;
                }

                int significantBits = bitStream.getNextNBitsAsInteger(length);
                int shiftedBits = significantBits << trailingZeroes;
                int value = previousValue ^ shiftedBits;
                floatValues.add(Float.intBitsToFloat(value));
                previousValue = value;
            }
        }
        return floatValues;
    }
}
