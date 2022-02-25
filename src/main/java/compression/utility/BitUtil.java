package compression.utility;

public class BitUtil {

    public static String int2Bits(int i){
        return Integer.toBinaryString(i);
    }

    public static int bits2Int(String bits) {
        return Integer.parseInt(bits, 2);
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
