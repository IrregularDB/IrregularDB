package utility;

public class BitUtil {

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
}
