package compression.utility.BitStream;

public interface BitStream {

    int getSize();

    String getNBits(int n);

    boolean hasNNext(int n);
}
