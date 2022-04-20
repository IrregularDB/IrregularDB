package compression.utility;

public class LongToInt {

    /***
     * Calculates difference between two timestamps and returns them as an integer if they can fit within an integer.
     * Returns null when they could not bet fitted.
     */
    public static Integer calculateDifference(long timestamp1, long timestamp2) {
        long diff = timestamp1 - timestamp2;
        return castToInt(diff);
    }


    public static Integer castToInt(long l) {
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
            return null;
        } else {
            return (int)l;
        }
    }
}
