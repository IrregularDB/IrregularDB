package compression.utility.BitStream;

import compression.utility.BitUtil;

import java.nio.ByteBuffer;

/**
 * This seems super duper un optimised
 */
public class BitStreamOld implements BitStream {
    private int index;
    private final String allBits;
    private final int size;

    /**
     * We cant know without having a number of timestamps how many of the last bits are padding
     */
    public BitStreamOld(ByteBuffer byteBuffer) {
        this.index = 0;
        StringBuilder stringBuilder = new StringBuilder();
        for (byte b : byteBuffer.array()) {
            stringBuilder.append(BitUtil.byte2Bits(b));
        }
        this.allBits = stringBuilder.toString();
        this.size = allBits.length();
    }

    public int getSize() {
        return size;
    }

    public String getNBits(int n) {
        if (n < 1) {
            throw new IllegalArgumentException("You must read at least one bit");
        }
        if (n + (index - 1) < size) {
            String output = allBits.substring(index, index + n);
            index += n;
            return output;
        } else {
            throw new IndexOutOfBoundsException("There arent that many bits left in the stream");
        }
    }

    public boolean hasNNext(int n){
        return (index-1) + n < size;
    }
}
