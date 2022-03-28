// The implementation of this bit stream is based on code
// published in relation to ModelarDB
// LINK: https://github.com/skejserjensen/ModelarDB

package compression.utility.BitStream;

import java.nio.ByteBuffer;

public class BitStreamNew implements BitStream {
    private final int size;
    private final ByteBuffer byteBuffer;
    private int bitsLeft;
    private byte currByte;


    public BitStreamNew(ByteBuffer byteBuffer) {
        this.byteBuffer = byteBuffer;
        this.byteBuffer.position(0);
        this.size = byteBuffer.capacity() * Byte.SIZE;
        this.readNextByte();
    }

    @Override
    public int getSize() {
        return this.size;
    }

    @Override
    public int getNextNBitsAsInteger(int n) {
        if (n < 1 || n > 32) {
            throw new IllegalArgumentException("You must read at least one bit and at max 32 bits as an integer");
        } else if (!hasNNext(n)) {
            throw new IndexOutOfBoundsException("There is not" + n + "bits left in the stream");
        }

        int value = 0;
        while (n > 0) {
            if (n > this.bitsLeft || n == Byte.SIZE) {
                //Reads only the bitsLeft least significant bits using the mask
                byte mask = getByteMask(this.bitsLeft);
                byte maskedByte = (byte) (this.currByte & mask);
                value = getUpdatedValue(value, this.bitsLeft, maskedByte);
                n -= this.bitsLeft;
                this.bitsLeft = 0;
            } else {
                //Shifts to correct position. Then uses the mask to read only least significant bits
                byte mask = getByteMask(n);
                byte maskedByte = (byte) ((this.currByte >>> (this.bitsLeft - n)) & mask);
                value = getUpdatedValue(value, n, maskedByte);
                this.bitsLeft -= n;
                n = 0;
            }
            //The current byte has been exhausted and we move to the next
            if (this.bitsLeft == 0 && this.byteBuffer.hasRemaining()) {
                readNextByte();
            }
        }
        return value;
    }

    private byte getByteMask(int amtBitsWrittenInCurrentIteration){
        /* To get the mask we shift a 1 left until we reach amtBitsWrittenInCurrentIteration then we minus 1 to
         convert all the bits to the right of it to ones.
         E.g. if amtBitsWrittenInCurrentIteration = 4, Then we get the following mask:
             0b00000001 << 4 = 0b00010000
             0b00010000 - 1  = 0b00001111 */
        return (byte)((0b00000001 << amtBitsWrittenInCurrentIteration) - 1);
    }

    private int getUpdatedValue(int prevValue, int amtBitsWrittenInCurrentIteration, byte maskedByte) {
        /* We move the value bits left bits to the left and then add in the masked byte as an integer e.g.
              valueBefore  = ... 0000 0000 1010, bitsLeftInByte = 4
              shiftedValue = ... 0000 0000 1010 << 4 = ... 0000 1010 0000
              The masked byte is then "0000 0101". Giving:
              value        = ... 0000 1010 0000 + 0000 0101 = ... 0000 1010 0101
         */
        int shiftedValue = (prevValue << amtBitsWrittenInCurrentIteration);
        return shiftedValue + byteToInt(maskedByte);
    }

    private int byteToInt(byte b){
        /* This method masks away all the unnecessary bits from for example:
           -1 = 1111 1111 1111 1111 1111 1111 1111 1111
           By & them with the 0xFF mask */
        return b & 0xFF;
    }

    @Override
    public boolean hasNNext(int n) {
        // The position of the byte buffer already points to the next byte)
        int amtBytesLeft = byteBuffer.capacity() - byteBuffer.position();
        int amtBitsLeftInTotal = bitsLeft + amtBytesLeft * Byte.SIZE;

        return n <= amtBitsLeftInTotal;
    }

    private void readNextByte() {
        this.currByte = this.byteBuffer.get();
        this.bitsLeft = Byte.SIZE;
    }
}
