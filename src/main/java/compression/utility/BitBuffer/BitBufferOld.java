// The idea behind this BitBuffer is based on the one published in relation to ModelarDB
// LINK: https://github.com/skejserjensen/ModelarDB

package compression.utility.BitBuffer;

import compression.utility.BitUtil;

import java.nio.ByteBuffer;


/**
 * This class is used to have an automatically extending byte buffer
 * that can keep track of bit values
 */
public class BitBufferOld extends BitBuffer {
    private ByteBuffer byteBuffer;
    private StringBuilder currByte;
    private final String valueUsedToFinishBitBuffer;

    public BitBufferOld(boolean finishWithOnes) {
        int initialByteBufferSize = 16;
        this.byteBuffer = ByteBuffer.allocate(initialByteBufferSize);
        this.currByte = new StringBuilder();

        if (finishWithOnes) {
            this.valueUsedToFinishBitBuffer = "1";
        } else {
            this.valueUsedToFinishBitBuffer = "0";
        }
    }

    @Override
    protected ByteBuffer getByteBuffer() {
        return this.byteBuffer;
    }

    @Override
    protected void setByteBuffer(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
    }

    @Override
    public void writeFalseBit() {
        writeBit('0');
    }

    @Override
    public void writeTrueBit() {
        writeBit('1');
    }

    @Override
    public void writeIntUsingNBits(int i, int n) {
        String s = BitUtil.int2Bits(i, n);
        writeBitString(s);
    }

    @Override
    public int bitsLeftInCurrentByte(){
        if (currByte.isEmpty()) {
            return 0;
        }
        return Byte.SIZE - currByte.length();
    }

    @Override
    protected void handledUnfinishedByte() {
        while (currByte.length() < Byte.SIZE) {
            currByte.append(valueUsedToFinishBitBuffer);
        }
        flushCurrentByteToBuffer();
    }


    private void writeBitString(String bitString) {
        bitString.chars().forEach(c -> writeBit((char) c));
    }

    private void writeBit(char bit) {
        currByte.append(bit);
        if (currByte.length() == Byte.SIZE) {
            flushCurrentByteToBuffer();
        }
    }

    private void flushCurrentByteToBuffer() {
        if (!byteBuffer.hasRemaining()) {
            // We double up the byte buffer size instead of having to extend it after each new byte
            extendBufferWithNMoreBytes(byteBuffer.capacity());
        }
        // This hack is necessary as BYTE.parse does not really work that well with 8 bit values
        byteBuffer.put((byte)Integer.parseInt(currByte.toString(), 2));
        currByte = new StringBuilder();
    }
}
