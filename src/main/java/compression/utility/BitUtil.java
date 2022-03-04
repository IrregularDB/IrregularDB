package compression.utility;

public class BitUtil {

    public static String int2Bits(int i){
        return Integer.toBinaryString(i);
    }

    public static String int2Bits(int i, int amtBits) {
        String bitString = BitUtil.int2Bits(i);
        int amtBitsInValue = bitString.length();
        int zeroesToPad = amtBits - amtBitsInValue;
        if (zeroesToPad < 0) {
            throw new IllegalArgumentException("You tried to represent the integer: " + i + ". Using only: " + amtBits + ". Which is not possible");
        }
        return "0".repeat(zeroesToPad) + bitString;
    }

    // TODO: MOVE THIS TO THE TEST FOLDER AS IT IS NO LONGER USED IN THE PRODUCTION CODE
    public static int bits2Int(String bits) {
        if (bits.length() < 32) {
            return Integer.parseInt(bits, 2);
        } else if (bits.length() == 32) {
            // Necessary hack to parse 32 bit integers e.g. 1111 1111 1111 1111 1111 1111 1111 1111
            return (int)(Long.parseLong(bits, 2));
        } else {
            throw new IllegalArgumentException("You tried to convert an bit string that was larger than 32 bits");
        }
    }

    /**
     * @return this always returns an 8-bit representation of the byte.
     */
    public static String byte2Bits(byte b) {
        // This method masks away all the unnecessary bits from for example:
        // -1 = 1111 1111 1111 1111 1111 1111 1111 1111
        // By & them with the 0xFF mask
        int i = b & 0xFF;
        String byteString = Integer.toBinaryString(i);

        // Then if the string is too short it formats it to be 8 long
        return String.format("%8s", byteString).replace(' ', '0');
    }
}
