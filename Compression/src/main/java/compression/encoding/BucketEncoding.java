package compression.encoding;

import compression.utility.BitBuffer.BitBuffer;
import compression.utility.BitBuffer.BitBufferNew;
import compression.utility.BitStream.BitStream;

import java.util.ArrayList;
import java.util.List;

public class BucketEncoding {
    private final boolean useSignedBits;

    private static final byte SAME_VALUE_ENCODING = 0b00;
    private static final byte BUCKET_1_CONTROL_BITS = 0b01;
    private static final byte BUCKET_2_CONTROL_BITS = 0b10;
    private static final byte BUCKET_3_CONTROL_BITS = 0b11;

    private static final int BUCKET_1_BIT_SIZE = 9;
    private static final int BUCKET_2_BIT_SIZE = 16;
    private static final int BUCKET_3_BIT_SIZE = 31;
    protected static final int AMT_CONTROL_BITS = 2;

    public static final int NEGATIVE_SIGNED_BIT = 0;
    public static final int POSITIVE_SIGNED_BIT = 1;

    protected final BitBuffer bitBuffer;

    public static int getSmallestNonZeroBucketSizeInBits(){
        return BUCKET_1_BIT_SIZE;
    }

    public BucketEncoding(boolean useSignedBits){
        this.bitBuffer = new BitBufferNew(true);
        this.useSignedBits = useSignedBits;
    }

    /**
     * @param readings we only support positive numbers
     */
    public BitBuffer encode(List<Integer> readings) {
        // We finish the byte with 1's as we can then in the decoding detect end of stream
        int previousReading = Integer.MIN_VALUE;
        for (int reading : readings) {
            if (reading == Integer.MIN_VALUE) {
                throw new IllegalArgumentException("We cannot encode Integer.MIN_VALUE");
            }
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

    private void encodeReading(int reading, int prevReading) {

        if (reading == prevReading) {
            writeControlBitsToBuffer(SAME_VALUE_ENCODING, bitBuffer);
        } else {
            encodeNumber(reading);
        }
    }

    protected void encodeNumber(int reading) {
        boolean negativeNumber = reading < 0;
        reading = negativeNumber ? -reading : reading;

        if (!useSignedBits && negativeNumber) {
            throw new IllegalArgumentException("You tried to store a negative number without using signed bits");
        }

        int amtSignificantBits = Integer.SIZE - Integer.numberOfLeadingZeros(reading);
        if (amtSignificantBits <= BUCKET_1_BIT_SIZE) {
            writeControlBitsToBuffer(BUCKET_1_CONTROL_BITS, bitBuffer);
            encodeSignedBit(useSignedBits, negativeNumber);
            bitBuffer.writeIntUsingNBits(reading, BUCKET_1_BIT_SIZE);
        } else if (amtSignificantBits <= BUCKET_2_BIT_SIZE) {
            writeControlBitsToBuffer(BUCKET_2_CONTROL_BITS, bitBuffer);
            encodeSignedBit(useSignedBits, negativeNumber);
            bitBuffer.writeIntUsingNBits(reading, BUCKET_2_BIT_SIZE);
        } else if (amtSignificantBits <= BUCKET_3_BIT_SIZE) {
            writeControlBitsToBuffer(BUCKET_3_CONTROL_BITS, bitBuffer);
            encodeSignedBit(useSignedBits, negativeNumber);
            bitBuffer.writeIntUsingNBits(reading, BUCKET_3_BIT_SIZE);
        } else {
            throw new IllegalArgumentException("Amount of bits greater than bucket allows (you probably tried to insert a negative number)");
        }
    }

    private void encodeSignedBit(boolean useSignedBits, boolean negativeNumber) {
        if (useSignedBits) {
            if (negativeNumber) {
                bitBuffer.writeIntUsingNBits(NEGATIVE_SIGNED_BIT, 1);
            } else {
                bitBuffer.writeIntUsingNBits(POSITIVE_SIGNED_BIT, 1);
            }
        }
    }

    private static void writeControlBitsToBuffer(byte controlBits, BitBuffer bitBuffer) {
        bitBuffer.writeIntUsingNBits(controlBits, AMT_CONTROL_BITS);
    }

    public static List<Integer> decode(BitStream bitStream, boolean usesSignedBits) {
        ArrayList<Integer> integers = new ArrayList<>();

        int lastInteger = Integer.MIN_VALUE;

        while (bitStream.hasNNext(AMT_CONTROL_BITS)) {
            lastInteger = decodeInteger(lastInteger, bitStream, usesSignedBits);

            if (lastInteger == Integer.MIN_VALUE){
                //this indicates end of stream, and remaining bits of stream are without significance
                break;
            }
            integers.add(lastInteger);
        }
        return integers;
    }

    private static Integer decodeInteger(int lastInteger, BitStream bitStream, boolean usesSignedBits){
        byte controlBits = (byte) bitStream.getNextNBitsAsInteger(AMT_CONTROL_BITS);

        if (SAME_VALUE_ENCODING == controlBits) {
            if (lastInteger == Integer.MIN_VALUE) {
                throw new IllegalStateException("BucketEncoding:decode: \"Error - first value cannot have control bit '00'\"");
            }
            return lastInteger;
        } else {
            int amtBitsInBucket = controlBitsToLength(controlBits, usesSignedBits);
            int amtNeededBits = usesSignedBits ? amtBitsInBucket + 1 : amtBitsInBucket;
            if (bitStream.hasNNext(amtNeededBits)) {
                if (usesSignedBits) {
                    return handleSignedValues(bitStream, amtBitsInBucket);
                } else {
                    return bitStream.getNextNBitsAsInteger(amtBitsInBucket);
                }
            } else {
                //this indicates end of stream, and remaining bits of stream are without significance
                return Integer.MIN_VALUE;
            }
        }
    }

    private static int handleSignedValues(BitStream bitStream, int amtBitsInBucket) {
        byte signedBit = (byte) bitStream.getNextNBitsAsInteger(1);

        int integer = bitStream.getNextNBitsAsInteger(amtBitsInBucket);
        if (signedBit == NEGATIVE_SIGNED_BIT){
            integer = integer * -1;
        }
        return integer;
    }

    private static int controlBitsToLength(byte controlBits, boolean usesSignedBits) {
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
