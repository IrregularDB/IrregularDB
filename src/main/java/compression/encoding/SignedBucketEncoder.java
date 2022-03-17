package compression.encoding;

import compression.utility.BitBuffer.BitBuffer;
import compression.utility.BitStream.BitStream;

import java.util.ArrayList;
import java.util.List;

public class SignedBucketEncoder extends BucketEncoding{

    public static final int NEGATIVE_SIGNED_BIT = 0;
    public static final int POSITIVE_SIGNED_BIT = 1;

    @Override
    public BitBuffer encode(List<Integer> readings) {
        for (int reading : readings){
            encodeNumber(reading);
        }
        return bitBuffer;
    }

    @Override
    protected void encodeNumber(int reading) {
        if (reading == Integer.MIN_VALUE) {
            throw new IllegalArgumentException("Integer.MIN_VALUE not allowed for signed bucked encoding");
        }
        boolean negativeNumber = reading < 0;
        if (negativeNumber) {
            bitBuffer.writeIntUsingNBits(NEGATIVE_SIGNED_BIT, 1);
            super.encodeNumber(-reading);
        } else {
            bitBuffer.writeIntUsingNBits(POSITIVE_SIGNED_BIT, 1);
            super.encodeNumber(reading);
        }
    }

    public List<Integer> decodeSigned(BitStream bitStream) {
        ArrayList<Integer> integers = new ArrayList<>();

        byte signedBit;
        int lastInteger = -1;

        while (bitStream.hasNNext(AMT_CONTROL_BITS + 1)){
            signedBit = (byte) bitStream.getNextNBitsAsInteger(1);
            lastInteger = decodeInteger(lastInteger, bitStream);

            if (Integer.MIN_VALUE == lastInteger){
                break;
            } else {
                if (signedBit == NEGATIVE_SIGNED_BIT){
                    integers.add(-lastInteger);
                } else {
                    integers.add(lastInteger);
                }
            }
        }

        return integers;
    }
}
