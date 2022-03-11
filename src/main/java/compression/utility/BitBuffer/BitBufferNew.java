// The implementation of this bit stream is based on code
// published in relation to ModelarDB
// LINK: https://github.com/skejserjensen/ModelarDB

package compression.utility.BitBuffer;

import java.nio.ByteBuffer;

public class BitBufferNew extends BitBuffer {
    private int bitsLeft;
    private byte currByte;
    private ByteBuffer byteBuffer;
    private final boolean finishWithOnes;

    public BitBufferNew(boolean finishWithOnes) {
        int initialByteBufferSize = 16;
        this.byteBuffer = ByteBuffer.allocate(initialByteBufferSize);
        this.finishWithOnes = finishWithOnes;
        resetCurrentByte();
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
    public int bitsLeftInCurrentByte() {
        return bitsLeft % 8;
    }

    @Override
    public void writeFalseBit() {
        // We do nothing as the current bit is defaulted to zero
        this.bitsLeft--;

        if (this.bitsLeft == 0) {
            flushCurrentByteToBuffer();
        }
    }

    @Override
    public void writeTrueBit() {
        // We shift a 1 left until we reach the current bit and OR onto our current byte
        this.currByte |= (1 << (this.bitsLeft - 1));
        this.bitsLeft--;

        if (this.bitsLeft == 0) {
            flushCurrentByteToBuffer();
        }
    }

    @Override
    public void writeIntUsingNBits(int i, int n) {

        while (n > 0) {
            int amtBitsToWriteInCurrentIteration;
            if (n > this.bitsLeft) { // Not enough bits left in current byte
                amtBitsToWriteInCurrentIteration = this.bitsLeft;
                // First we shift the value right so we only have the bits-left amount of bits from the front of i
                int howFarToShiftRight = n - this.bitsLeft;
                int shiftedValue = (i >> howFarToShiftRight);

                byte mask = getByteMask(bitsLeft);
                this.currByte |= (byte) (shiftedValue & mask);
            } else { // Enough bits left in current byte
                amtBitsToWriteInCurrentIteration = n;
                int howFarToShiftLeft = this.bitsLeft - n;
                // We shift the current value to the left to get it to start from bits left
                int shiftedValue = (i << howFarToShiftLeft);
                this.currByte |= (byte) shiftedValue;
            }
            n -= amtBitsToWriteInCurrentIteration;
            this.bitsLeft -= amtBitsToWriteInCurrentIteration;

            //If we reached the last bit a new byte is allocated
            if (this.bitsLeft == 0) {
                flushCurrentByteToBuffer();
            }
        }
    }

    private byte getByteMask(int bitsLeftInByte){
        /* To get the mask we shift a 1 left until we reach bits left then we minus 1 to convert all the bits
           to the right of it to ones. E.g. if bitsLeft = 4, Then we get the following mask:
             0b00000001 << 4 = 0b00010000
             0b00010000 - 1  = 0b00001111 */
        return (byte)((0b00000001 << bitsLeftInByte) - 1);
    }

    @Override
    protected void handleUnfinishedByte() {
        while (this.bitsLeftInCurrentByte() != 0) {
            if (finishWithOnes) {
                writeTrueBit();
            } else {
                writeFalseBit();
            }
        }
    }

    private void flushCurrentByteToBuffer() {
        if (!byteBuffer.hasRemaining()) {
            // We double up the byte buffer size instead of having to extend it after each new byte
            extendBufferWithNMoreBytes(byteBuffer.capacity());
        }
        byteBuffer.put(currByte);
        resetCurrentByte();
    }

    private void resetCurrentByte() {
        currByte = 0;
        this.bitsLeft = Byte.SIZE;
    }
}
