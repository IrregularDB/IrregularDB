package compression.utility.BitStream;

public interface BitStream {
    int getSize();

    int getNextNBitsAsInteger(int n);

    boolean hasNNext(int n);
}
