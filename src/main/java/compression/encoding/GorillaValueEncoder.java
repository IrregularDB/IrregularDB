package compression.encoding;

import compression.utility.BitBuffer;

import java.util.List;

public class GorillaValueEncoder {
    public static BitBuffer encode(List<Float> values) {
        BitBuffer bitBuffer = new BitBuffer(16);
        int previousLeadingZeroes;
        int previouslenght;

        // Get BIT representation of the float value
        var previousValue =  Float.floatToIntBits(values.get(0));

        for (int i = 1; i < values.size(); i++) {
            var currValue = Float.floatToIntBits(values.get(i));
            int xor = currValue ^ previousValue;
            if (xor == 0) {
                bitBuffer.writeBit('0');
            } else if ()

        }




        throw new RuntimeException("Not implemented");
    }


    public static List<Float> decode(BitBuffer bitBuffer) {
        throw new RuntimeException("Not implemented");
    }
}
