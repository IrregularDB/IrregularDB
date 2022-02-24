// The idea behind this BitBuffer is based on the one published in relation to ModelarDB
// LINK: https://github.com/skejserjensen/ModelarDB

package compression.utility;

import java.nio.ByteBuffer;


/**
 * This class is used to have an automatically extending byte buffer
 * that can keep track of bit values
 */
public class BitBuffer {
    private ByteBuffer byteBuffer;
    private StringBuilder currByte;

    public BitBuffer(int initialByteBufferSize) {
        if (initialByteBufferSize < 1) {
            throw new IllegalArgumentException("The initial buffer size should at least be 1");
        }
        this.byteBuffer = ByteBuffer.allocate(initialByteBufferSize);
        this.currByte = new StringBuilder();
    }

    public ByteBuffer getByteBuffer() {
        if (currByte.length() != 0) { // We have an unfinished byte
            handledUnfinishedByte();
        }
        if (byteBuffer.hasRemaining()) { // We have allocated more bytes than needed
            shortenBufferToSizeN(this.byteBuffer.position());
        }
        return byteBuffer;
    }

    public void putFloat(float value){
        if (byteBuffer.remaining() < 4) {
            extendBufferWithNMoreBytes(4);
        }
        byteBuffer.putFloat(value);
    }

    public void writeBit(char bit) {
        currByte.append(bit);
        if (currByte.length() == Byte.SIZE) {
            flushCurrentByteToBuffer();
        }
    }

    public void writeBitString(String bitString) {
        bitString.chars().forEach(c -> writeBit((char) c));
    }

    private void handledUnfinishedByte() {
        while (currByte.length() < Byte.SIZE) {
            currByte.append("0");
        }
        flushCurrentByteToBuffer();
    }

    private void shortenBufferToSizeN(int n) {
        ByteBuffer shortenedBuffer = ByteBuffer.allocate(n);
        this.byteBuffer.flip();
        shortenedBuffer.put(this.byteBuffer);
        this.byteBuffer = shortenedBuffer;
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

    private void extendBufferWithNMoreBytes(int n) {
        ByteBuffer extendedBuffer = ByteBuffer.allocate(byteBuffer.capacity() + n);
        this.byteBuffer.flip();
        extendedBuffer.put(this.byteBuffer);
        this.byteBuffer = extendedBuffer;
    }

    public BitStream getBitStream(){
        return new BitStream(getByteBuffer());
    }

    /**
     * This seems super duper un optimised
     */
    public static class BitStream{
        private int index;
        private final String allBits;
        private final int size;

        /**
         * We cant know without having a number of timestamps how many of the last bits are padding
         */
        public BitStream(ByteBuffer byteBuffer) {
            this.index = 0;
            StringBuilder stringBuilder = new StringBuilder();
            for (byte b : byteBuffer.array()) {
                stringBuilder.append(BitUtil.int2Bits(b));
            }
            this.allBits = stringBuilder.toString();
            this.size = allBits.length();
        }

        public String getNBits(int n) {
            if (n < 1) {
                throw new IllegalArgumentException("You must read at least one bit");
            }
            if (n + index < size) {
                index += n;
                 return allBits.substring(index - n, index);
            } else {
                throw new IndexOutOfBoundsException("There arent that many bits left in the stream");
            }
        }

        public boolean hasNNext(int n){
            return index + n < size;
        }

    }
}
