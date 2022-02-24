package compression;

import compression.utility.BitBuffer;

import java.util.List;

public interface Encoding<T> {

    BitBuffer encode(List<T> readings);

    List<T> decode(BitBuffer bitBuffer);
}
