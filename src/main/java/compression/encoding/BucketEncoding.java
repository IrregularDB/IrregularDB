package compression.encoding;

import compression.utility.*;
import compression.utility.BitBuffer.BitBuffer;
import compression.utility.BitBuffer.BitBufferNew;
import compression.utility.BitStream.BitStream;

import java.util.ArrayList;
import java.util.List;

public class BucketEncoding {

    private static final byte SAME_VALUE_ENCODING = 0b00;
    private static final byte BUCKET_1_CONTROL_BITS = 0b01;
    private static final byte BUCKET_2_CONTROL_BITS = 0b10;
    private static final byte BUCKET_3_CONTROL_BITS = 0b11;

    private static final int BUCKET_1_BIT_SIZE = 9;
    private static final int BUCKET_2_BIT_SIZE = 16;
    private static final int BUCKET_3_BIT_SIZE = 31;
    private static final int AMT_CONTROL_BITS = 2;

    /**
     * @param readings we only support positive numbers
     */
    public static BitBuffer encode(List<Integer> readings, boolean handleSignedValues) {
        // We finish the byte with 1's as we can then in the decoding detect end of stream
        BitBuffer bitBuffer = new BitBufferNew(true);
        Integer previousReading = null;
        for (Integer reading : readings) {
            encodeReading(bitBuffer, reading, previousReading, handleSignedValues);
            previousReading = reading;
        }

        return bitBuffer;
    }

    private static void encodeReading(BitBuffer bitBuffer, Integer reading, Integer prevReading, boolean handleSignedValues) {
        if (reading.equals(prevReading)) {
            writeControlBitsToBuffer(SAME_VALUE_ENCODING, bitBuffer);
        } else {
            encodeNumber(reading, bitBuffer, handleSignedValues);
        }
    }

    private static void encodeNumber(int reading, BitBuffer bitBuffer, boolean handleSignedValues) {
        boolean negativeNumber = reading < 0;
        if (negativeNumber) {
            
        }

        int amtSignificantBits = Integer.SIZE - Integer.numberOfLeadingZeros(reading);

        if (amtSignificantBits <= BUCKET_1_BIT_SIZE) {
            writeControlBitsToBuffer(BUCKET_1_CONTROL_BITS, bitBuffer);
            bitBuffer.writeIntUsingNBits(reading, BUCKET_1_BIT_SIZE);
        } else if (amtSignificantBits <= BUCKET_2_BIT_SIZE) {
            writeControlBitsToBuffer(BUCKET_2_CONTROL_BITS, bitBuffer);
            bitBuffer.writeIntUsingNBits(reading, BUCKET_2_BIT_SIZE);
        } else if (amtSignificantBits <= BUCKET_3_BIT_SIZE) {
            writeControlBitsToBuffer(BUCKET_3_CONTROL_BITS, bitBuffer);
            bitBuffer.writeIntUsingNBits(reading, BUCKET_3_BIT_SIZE);
        } else {
            throw new IllegalArgumentException("Amount of bits greater than bucket allows (you probably tried to insert a negative number)");
        }
    }

    private static void writeControlBitsToBuffer(byte controlBits, BitBuffer bitBuffer) {
        bitBuffer.writeIntUsingNBits(controlBits, AMT_CONTROL_BITS);
    }

    public static List<Integer> decode(BitStream bitStream) {
        ArrayList<Integer> integers = new ArrayList<>();

        byte controlBits;
        int lastInteger = -1;

        while (bitStream.hasNNext(AMT_CONTROL_BITS)) {
            controlBits = (byte) bitStream.getNextNBitsAsInteger(AMT_CONTROL_BITS);

            if (SAME_VALUE_ENCODING == controlBits) {
                if (lastInteger == -1) {
                    throw new IllegalStateException("BucketEncoding:decode: \"Error - first value cannot have control bit '00'\"");
                }
                integers.add(lastInteger);
            } else {
                int amtBitsInBucket = controlBitsToLength(controlBits);
                if (bitStream.hasNNext(amtBitsInBucket)) {
                    lastInteger = bitStream.getNextNBitsAsInteger(amtBitsInBucket);
                    integers.add(lastInteger);
                } else {
                    //this indicates end of stream, and remaining bits of stream are without significance
                    break;
                }
            }
        }
        return integers;
    }

    private static int controlBitsToLength(byte controlBits) {
        switch (controlBits) {
            case BUCKET_1_CONTROL_BITS -> {
                return BUCKET_1_BIT_SIZE;
            }
            case BUCKET_2_CONTROL_BITS -> {
                return BUCKET_2_BIT_SIZE;
            }
            case BUCKET_3_CONTROL_BITS -> {
                return BUCKET_3_BIT_SIZE;
            }
            default ->
                throw new IllegalArgumentException("unknown controlBits");
        }
    }
}
