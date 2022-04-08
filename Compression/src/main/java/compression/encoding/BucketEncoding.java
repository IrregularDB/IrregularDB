package compression.encoding;

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
    protected static final int AMT_CONTROL_BITS = 2;
    protected final BitBuffer bitBuffer;

    public static int getSmallestNonZeroBucketSizeInBits(){
        return BUCKET_1_BIT_SIZE;
    }

    public BucketEncoding(){
        bitBuffer = new BitBufferNew(true);
    }

    /**
     * @param readings we only support positive numbers
     */
    public BitBuffer encode(List<Integer> readings) {
        // We finish the byte with 1's as we can then in the decoding detect end of stream
        Integer previousReading = null;
        for (Integer reading : readings) {
            encodeReading(reading, previousReading);
            previousReading = reading;
        }

        return bitBuffer;
    }

    /**
     * we don't add the last bucket as it is the largest bucket
     */
    public List<Integer> getMaxAbsoluteValuesOfResizeableBuckets() {
        List<Integer> maxValues = new ArrayList<>();

        // We have bucket_0, which is same value
        maxValues.add(0);

        maxValues.add((int)((long) Math.pow(2, BUCKET_1_BIT_SIZE)) - 1);
        maxValues.add((int)((long) Math.pow(2, BUCKET_2_BIT_SIZE)) - 1);
        return maxValues;
    }

    private void encodeReading(Integer reading, Integer prevReading) {
        if (reading.equals(prevReading)) {
            writeControlBitsToBuffer(SAME_VALUE_ENCODING, bitBuffer);
        } else {
            encodeNumber(reading);
        }
    }

    protected void encodeNumber(int reading) {

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

        int lastInteger = -1;

        while (bitStream.hasNNext(AMT_CONTROL_BITS)) {
            lastInteger = decodeInteger(lastInteger, bitStream);

            if (lastInteger == Integer.MIN_VALUE){
                //this indicates end of stream, and remaining bits of stream are without significance
                break;
            }
            integers.add(lastInteger);
        }
        return integers;
    }

    protected static Integer decodeInteger(int lastInteger, BitStream bitStream){
        byte controlBits = (byte) bitStream.getNextNBitsAsInteger(AMT_CONTROL_BITS);

        if (SAME_VALUE_ENCODING == controlBits) {
            if (lastInteger == Integer.MIN_VALUE) {
                throw new IllegalStateException("BucketEncoding:decode: \"Error - first value cannot have control bit '00'\"");
            }
            return lastInteger;
        } else {
            int amtBitsInBucket = controlBitsToLength(controlBits);
            if (bitStream.hasNNext(amtBitsInBucket)) {
                return bitStream.getNextNBitsAsInteger(amtBitsInBucket);
            } else {
                //this indicates end of stream, and remaining bits of stream are without significance
                return Integer.MIN_VALUE;
            }
        }
    }

    protected static int controlBitsToLength(byte controlBits) {
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
