package compression.utility;

public class BitUtil {

    public static String int2Bits(int i){
        return Integer.toBinaryString(i);
    }

    public static int bits2Int(String bits) {
        return Integer.parseInt(bits, 2);
    }

}
