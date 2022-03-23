package compression.encoding;

import compression.utility.BitBuffer.BitBuffer;
import compression.utility.BitBuffer.BitBufferNew;
import compression.utility.BitStream.BitStream;
import compression.utility.BitStream.BitStreamNew;

import java.nio.ByteBuffer;
import java.util.List;

public class SingleIntEncoding {
    public static ByteBuffer encode(int integer) {
        BitBuffer bitBuffer = new BitBufferNew(true);

        int amtLeadingZeroes = Integer.numberOfLeadingZeros(integer);
        int length = Integer.SIZE - amtLeadingZeroes;

        int amtZeroesToPad = amtLeadingZeroes % Byte.SIZE;
        bitBuffer.writeIntUsingNBits(0, amtZeroesToPad);

        bitBuffer.writeIntUsingNBits(integer, length);
        return bitBuffer.getFinishedByteBuffer();
    }


    public static Integer decode(ByteBuffer byteBuffer) {
        BitStream bitStream = new BitStreamNew(byteBuffer);

        int size = bitStream.getSize();
        return bitStream.getNextNBitsAsInteger(size);
    }

}
