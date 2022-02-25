package compression.encoding;

import compression.utility.BitBuffer;
import compression.utility.BitStream;
import compression.utility.BitUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class BucketEncoding {

    private static final String SAME_VALUE_ENCODING = "00";
    private static final int BUCKET_1_BIT_SIZE = 9;
    private static final int BUCKET_2_BIT_SIZE = 16;
    private static final int BUCKET_3_BIT_SIZE = 32;
    private static final int AMT_CONTROL_BITS = 2;

    public static BitBuffer encode(List<Integer> readings) {
        BitBuffer bitBuffer = new BitBuffer(4);
        Integer previousReading = null;
        for (Integer reading : readings) {
            String encodeReading = encodeReading(reading, previousReading);
            previousReading = reading;
            bitBuffer.writeBitString(encodeReading);
        }

        finalizeBuffer(bitBuffer);

        return bitBuffer;
    }

    /**
     * There can be an unfinished byte. In order to handle this we write 11 as control bits that require more bits than are available in the stream. This indicates end of stream
     * @param bitBuffer
     */
    private static void finalizeBuffer(BitBuffer bitBuffer) {
        if (bitBuffer.bitsLeftInCurrentByte() > 1) {
            bitBuffer.writeBitString("11");
        }
    }

    private static String encodeReading(Integer reading, Integer prevReading) {
        if (reading.equals(prevReading)) {
            return SAME_VALUE_ENCODING;
        }
        String significantBits = BitUtil.int2Bits(reading);
        return encodeNumber(significantBits);
    }

    private static String encodeNumber(String significantBits) {
        int amtSignificantBits = significantBits.length();

        String controlBits;
        int zeroesToPad;
        if (amtSignificantBits <= BUCKET_1_BIT_SIZE) {
            controlBits = "01";
            zeroesToPad = BUCKET_1_BIT_SIZE - amtSignificantBits;
        } else if (amtSignificantBits <= BUCKET_2_BIT_SIZE) {
            controlBits = "10";
            zeroesToPad = BUCKET_2_BIT_SIZE - amtSignificantBits;
        } else if (amtSignificantBits <= BUCKET_3_BIT_SIZE) {
            controlBits = "11";
            zeroesToPad = BUCKET_3_BIT_SIZE - amtSignificantBits;
        } else {
            throw new RuntimeException("Value greater than bucket allows");
        }

        return controlBits + "0".repeat(zeroesToPad) + significantBits;
    }

    public static List<Integer> decode(BitStream bitStream) {
        ArrayList<Integer> integers = new ArrayList<>();

        String controlBits;
        int lastInteger = -1;

        while (bitStream.hasNNext(AMT_CONTROL_BITS)) {
            controlBits = bitStream.getNBits(AMT_CONTROL_BITS);

            if ("00".equals(controlBits)) {
                if (lastInteger == -1) {
                    throw new IllegalStateException("BucketEncoding:decode: \"Error - first value cannot have control bit '00'\"");
                }
                integers.add(lastInteger);
            } else {
                int bitsInBucket = controlBitsToLength(controlBits);
                if (bitStream.hasNNext(bitsInBucket)) {
                    lastInteger = BitUtil.bits2Int(bitStream.getNBits(bitsInBucket));
                    integers.add(lastInteger);
                } else {
                    //this indicates end of stream, and remaining bits of stream are without significance
                    break;
                }
            }
        }
        return integers;
    }

    private static int controlBitsToLength(String controlBits) {
        switch (controlBits) {
            case "01" -> {
                return BUCKET_1_BIT_SIZE;
            }
            case "10" -> {
                return BUCKET_2_BIT_SIZE;
            }
            case "11" -> {
                return BUCKET_3_BIT_SIZE;
            }
            default ->
                throw new IllegalArgumentException("unknown controlBits");
        }
    }
}
