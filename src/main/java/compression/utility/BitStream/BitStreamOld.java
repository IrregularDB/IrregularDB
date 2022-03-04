package compression.utility.BitStream;

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
            stringBuilder.append(byte2Bits(b));
        }
        this.allBits = stringBuilder.toString();
        this.size = allBits.length();
    }

    private String byte2Bits(byte b) {
        // This method masks away all the unnecessary bits from for example:
        // -1 = 1111 1111 1111 1111 1111 1111 1111 1111
        // By & them with the 0xFF mask
        int i = b & 0xFF;
        String byteString = Integer.toBinaryString(i);

        // Then if the string is too short it formats it to be 8 long
        return String.format("%8s", byteString).replace(' ', '0');
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public boolean hasNNext(int n){
        return (index-1) + n < size;
    }

    @Override
    public int getNextNBitsAsInteger(int n) {
        if (n < 32) {
            String bitPattern = getNBits(n);
            return Integer.parseInt(bitPattern, 2);
        } else if (n == 32) {
            // Necessary hack to parse 32 bit integers e.g. 1111 1111 1111 1111 1111 1111 1111 1111
            String bitPattern = getNBits(n);
            return (int)(Long.parseLong(bitPattern, 2));
        } else {
            throw new IllegalArgumentException("You tried to get more than 32 bits as an integer");
        }
    }

    private String getNBits(int n) {
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
}
