package utility;

public class BitPattern {
    String bitPattern;

    public BitPattern(String bitPattern) {
        this.bitPattern = removeSpace(bitPattern);
    }

    private String removeSpace(String string) {
        return string.replace(" ", "");
    }

    public int getAmtBits() {
        return bitPattern.length();
    }

    public int getIntRepresentation() {
        return BitUtil.bits2Int(bitPattern);
    }
}
